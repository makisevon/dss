#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate prost_derive;

use futures::executor::ThreadPool;

pub mod kvraft;
mod proto;
pub mod raft;

lazy_static::lazy_static! {
    static ref EXECUTOR: ThreadPool = ThreadPool::new().unwrap();
}
