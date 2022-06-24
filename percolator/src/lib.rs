#[allow(unused_imports)]
#[macro_use]
extern crate log;

mod client;
mod server;
mod service;
#[cfg(test)]
mod tests;

mod msg {
    include!(concat!(env!("OUT_DIR"), "/msg.rs"));
}
