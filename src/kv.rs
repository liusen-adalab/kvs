use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{self, Deserializer};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

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
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let value = store.get("key".to_owned())?;
/// assert_eq!(value, Some("value".to_owned()));
/// # Ok(())
/// # }
pub struct KvStore {
    path: PathBuf,
    index: BTreeMap<String, CommandPos>,
    uncompacted: u64,
    readers: HashMap<u64, MyReader<File>>,
    writer: MyWriter<File>,
    cur_gen: u64,
}

struct MyReader<R: Read + Seek> {
    inner: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> MyReader<R> {
    pub fn new(file: R) -> Self {
        MyReader {
            inner: BufReader::new(file),
            pos: 0,
        }
    }
}

impl<R: Read + Seek> Read for MyReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.inner.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Seek + Read> Seek for MyReader<R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.pos = self.inner.seek(pos)?;
        Ok(self.pos)
    }
}

struct MyWriter<R: Write + Seek> {
    inner: BufWriter<R>,
    pos: u64,
}

impl<W: Write + Seek> MyWriter<W> {
    fn new(path: W) -> Self {
        MyWriter {
            inner: BufWriter::new(path),
            pos: 0,
        }
    }
}
impl<W: Write + Seek> Write for MyWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

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
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();
        let mut uncompacted = 0u64;
        let gens = sorted_gen_list(&path)?;

        for &gen in &gens {
            let mut reader = MyReader::new(File::open(join_log(&path, gen))?);
            uncompacted += load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let cur_gen = gens.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, &mut readers, cur_gen)?;

        Ok(KvStore {
            path,
            index,
            uncompacted,
            readers,
            writer,
            cur_gen,
        })
    }

    /// Sets the string value of a string key to a string
    ///
    /// If the key already exsist, the previous value will be overwritten
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key, value);
        let serialize = serde_json::to_string(&command)?;

        if let Command::Set { key, .. } = command {
            let cur_pos = self.writer.pos;
            let len = self.writer.write(serialize.as_bytes())?;
            if let Some(old_cmd) = self
                .index
                .insert(key, CommandPos::new(self.cur_gen, cur_pos, len as u64))
            {
                self.uncompacted += old_cmd.len;
            }
            self.writer.flush()?;
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a string key
    ///
    /// Returns `None` if given string does not exsist
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");

            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let command = reader.take(cmd_pos.len);

            if let Command::Set { value, .. } = serde_json::from_reader(command)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// remove the given key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(old_cmd) = self.index.remove(&key) {
            serde_json::to_writer(&mut self.writer, &Command::rm(key))?;
            self.writer.flush()?;

            self.uncompacted += old_cmd.len;
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        let new_gen = self.cur_gen + 1;
        self.cur_gen += 2;

        let gens = sorted_gen_list(&self.path)?;

        let mut compact_writer = new_log_file(&self.path, &mut self.readers, new_gen)?;
        // self.writer = new_log_file(&mut self.path, &mut self.readers, self.cur_gen)?;

        let mut cur_pos = 0u64;
        for command in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&command.gen)
                .expect("cannot find log reader");
            reader.seek(SeekFrom::Start(command.pos))?;

            let mut new_reader = reader.take(command.len);
            let len = io::copy(&mut new_reader, &mut compact_writer)?;
            *command = CommandPos::new(new_gen, cur_pos, len);
            cur_pos += len;
        }
        compact_writer.flush()?;
        self.writer = compact_writer;

        for &gen in gens.iter() {
            let log_path = join_log(&self.path, gen);
            fs::remove_file(log_path)?;
            self.readers.remove(&gen);
        }
        self.uncompacted = 0;

        Ok(())
    }
}

fn new_log_file(
    path: &Path,
    readers: &mut HashMap<u64, MyReader<File>>,
    cur_gen: u64,
) -> Result<MyWriter<File>> {
    let path = join_log(path, cur_gen);
    let writer = MyWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?,
    );

    readers.insert(cur_gen, MyReader::new(File::open(&path)?));

    Ok(writer)
}

fn load(
    gen: u64,
    reader: &mut MyReader<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;

    let mut uncompacted = 0;
    let mut commands = Deserializer::from_reader(reader).into_iter::<Command>();

    let mut old_pos = 0;

    while let Some(command) = commands.next() {
        let new_pos = commands.byte_offset() as u64;

        match command? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) =
                    index.insert(key, CommandPos::new(gen, old_pos, new_pos - old_pos))
                {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
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
