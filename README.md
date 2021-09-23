这是一个来自 [PINGCAP talent plan](https://github.com/pingcap/talent-plan/blob/master/courses/rust/docs/lesson-plan.md) 课程的项目， 使用 rust 写的小型“键值对”数据库。

## Feature
- 支持 [sled](https://docs.rs/sled/0.34.7/sled/index.html) 引擎
- 使用 [log structured storage](http://blog.notdot.net/2009/12/Damn-Cool-Algorithms-Log-structured-storage) 持久化数据
- 多线程

## 使用
在项目根目录中打开一个终端
```
$ cargo run --bin kvs-server
```
在另一个终端中：
```
$ cargo run --bin kvs-client set hello world
$ cargo run --bin kvs-client get hello
```
