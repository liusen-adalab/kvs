use crate::{KvsError, Result};
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use serde_json::{self, Deserializer};
use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::{collections::BTreeMap, path::PathBuf};
use log::error;

use super::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` store string key/value pairs
///
/// key/value pairs are stored in a `HashMap` in memory and not persisted to disk
///
/// Example:
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()>{
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let value = store.get("key".to_owned())?;
/// assert_eq!(value, Some("value".to_owned()));
/// # Ok(())
/// # }
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, CommandPos>>,
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

struct KvStoreReader {
    path: Arc<PathBuf>,
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

struct KvStoreWriter {
    writer: BufWriterWithPos<File>,
    index: Arc<SkipMap<String, CommandPos>>,
    uncompacted: u64,
    cur_gen: u64,
    path: Arc<PathBuf>,
    reader: KvStoreReader,
}

struct BufReaderWithPos<R: Read + Seek> {
    inner: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub fn new(file: R) -> Self {
        BufReaderWithPos {
            inner: BufReader::new(file),
            pos: 0,
        }
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.inner.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Seek + Read> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.pos = self.inner.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<R: Write + Seek> {
    inner: BufWriter<R>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(path: W) -> Self {
        BufWriterWithPos {
            inner: BufWriter::new(path),
            pos: 0,
        }
    }
}
impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone, Copy)]
pub struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl CommandPos {
    pub fn new(gen: u64, pos: u64, len: u64) -> Self {
        CommandPos { gen, pos, len }
    }
}

#[derive(Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn rm(key: String) -> Self {
        Command::Remove { key }
    }
}

impl KvStore {
    /// Open a KvStore with given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let readers = BTreeMap::new();
        let index = Arc::new(SkipMap::new());
        let mut uncompacted = 0u64;
        let gens = sorted_gen_list(&path)?;

        for &gen in &gens {
            let mut reader = BufReaderWithPos::new(File::open(join_log(&path, gen))?);
            uncompacted += load(gen, &mut reader, &*index)?;
        }

        let cur_gen = gens.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, cur_gen)?;

        let reader = KvStoreReader {
            path: Arc::clone(&path),
            safe_point: Arc::new(AtomicU64::new(0)),
            readers: RefCell::new(readers),
        };
        let writer = KvStoreWriter {
            writer,
            index: Arc::clone(&index),
            uncompacted,
            cur_gen,
            path: Arc::clone(&path),
            reader: reader.clone(),
        };

        Ok(KvStore {
            path: Arc::clone(&path),
            index,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

fn new_log_file(path: &Path, cur_gen: u64) -> Result<BufWriterWithPos<File>> {
    let path = join_log(path, cur_gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?,
    );

    Ok(writer)
}

fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &SkipMap<String, CommandPos>,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;

    let mut uncompacted = 0;
    let mut commands = Deserializer::from_reader(reader).into_iter::<Command>();

    let mut old_pos = 0;

    while let Some(command) = commands.next() {
        let new_pos = commands.byte_offset() as u64;

        match command? {
            Command::Set { key, .. } => {
                if index.contains_key(&key) {
                    let old_entry = index.get(&key).unwrap();
                    uncompacted += old_entry.value().len;
                }
                index.insert(key, CommandPos::new(gen, old_pos, new_pos - old_pos));
            }
            Command::Remove { key } => {
                if let Some(old_entry) = index.remove(&key) {
                    uncompacted += old_entry.value().len;
                }
                uncompacted += new_pos - old_pos;
            }
        }

        old_pos = new_pos;
    }

    Ok(uncompacted)
}

fn join_log(path: &Path, gen: u64) -> PathBuf {
    path.join(format!("{}.log", gen))
}

fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gens: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|file| file.is_file() && file.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    gens.sort_unstable();

    Ok(gens)
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

impl KvStoreWriter {
    /// Gets the string value of a string key
    ///
    /// Returns `None` if given string does not exsist

    /// remove the given key
    fn remove(&mut self, key: String) -> Result<()> {
        if let Some(old_cmd) = self.index.remove(&key) {
            serde_json::to_writer(&mut self.writer, &Command::rm(key))?;
            self.writer.flush()?;

            self.uncompacted += old_cmd.value().len;
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Sets the string value of a string key to a string
    ///
    /// If the key already exsist, the previous value will be overwritten
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key, value);
        let position = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        if let Command::Set { key, .. } = command {
            if let Some(entry) = self.index.get(&key) {
                self.uncompacted += entry.value().len;
            }
            let cur_pos = self.writer.pos;
            self.index.insert(
                key,
                CommandPos::new(self.cur_gen, position, cur_pos - position),
            );
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let gens = sorted_gen_list(&self.path)?;
        let compaction_gen = self.cur_gen + 1;
        self.cur_gen = compaction_gen;

        let mut compact_writer = new_log_file(&self.path, compaction_gen)?;

        let mut cur_pos = 0u64;
        for entry in self.index.iter() {
            let len = self.reader.read_and(*entry.value(), |mut command| {
                Ok(io::copy(&mut command, &mut compact_writer)?)
            })?;
            let new_pos = CommandPos::new(compaction_gen, cur_pos, len);
            self.index.insert(entry.key().to_owned(), new_pos);
            cur_pos += len;
        }
        compact_writer.flush()?;
        self.writer = compact_writer;

        self.reader
            .safe_point
            .store(compaction_gen, Ordering::SeqCst);
        self.reader.close_stale_handler();

        // let reader = BufReaderWithPos::new(File::open(join_log(&self.path, compaction_gen))?);
        // self.reader.readers.borrow_mut().insert(compaction_gen, reader);

        for &gen in gens.iter() {
            let log_path = join_log(&self.path, gen);
            // fs::remove_file(log_path)?;
            if let Err(e) = fs::remove_file(&log_path) {
                error!("{:?} cannot be deleted: {}", log_path, e);
            }
        }
        self.uncompacted = 0;

        Ok(())
    }
}

impl KvStoreReader {
    fn read_command(&self, com_pos: CommandPos) -> Result<Command> {
        self.read_and(com_pos, |command| Ok(serde_json::from_reader(command)?))
    }

    fn read_and<F, R>(&self, com_pos: CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&com_pos.gen) {
            let reader = BufReaderWithPos::new(File::open(join_log(&*self.path, com_pos.gen))?);
            readers.insert(com_pos.gen, reader);
        }

        let reader = readers.get_mut(&com_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(com_pos.pos))?;
        let cmd_reader = reader.take(com_pos.len);

        f(cmd_reader)
    }

    fn close_stale_handler(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let &gen = readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= gen {
                break;
            }
            readers.remove(&gen);
        }
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            safe_point: self.safe_point.clone(),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}
