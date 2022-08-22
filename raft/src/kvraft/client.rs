use std::{
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use futures::{
    self,
    executor::{self, ThreadPool},
    FutureExt,
};
use futures_timer::Delay;

use crate::{
    proto::kvraftpb::{CommandId, GetArgs, GetCommand, KvClient, PutAppendArgs, PutAppendCommand},
    EXECUTOR,
};

pub struct Clerk {
    pub name: String,
    sequence: AtomicU64,

    clients: Vec<KvClient>,
    leader_id: AtomicUsize,

    executor: ThreadPool,
}

impl Clerk {
    const RPC_TIMEOUT: Duration = Duration::from_millis(500);
}

macro_rules! call {
    ($self:ident, $rpc:ident, $args:ident) => {{
        let mut clients = $self
            .clients
            .iter()
            .enumerate()
            .cycle()
            .skip($self.leader_id.load(Ordering::Relaxed));

        'client: loop {
            let (id, client) = clients.next().unwrap();
            loop {
                let mut rpc = client.$rpc(&$args).fuse();
                let reply = executor::block_on(async {
                    futures::select! {
                        reply = rpc => Some(reply),
                        _ = Delay::new(Self::RPC_TIMEOUT).fuse() => None,
                    }
                });

                if reply.is_none() {
                    $self.executor.spawn_ok(async { _ = rpc.await });
                    continue 'client;
                }

                let reply = match reply.unwrap() {
                    Ok(reply) if reply.is_leader => reply,
                    _ => continue 'client,
                };

                $self.leader_id.store(id, Ordering::Relaxed);
                if reply.error.is_empty() {
                    break 'client reply;
                }
            }
        }
    }};
}

impl Clerk {
    pub fn new(name: String, clients: Vec<KvClient>) -> Self {
        Self {
            name,
            sequence: 1.into(),

            clients,
            leader_id: 0.into(),

            executor: ThreadPool::clone(&EXECUTOR),
        }
    }

    pub fn get(&self, key: String) -> String {
        let args = GetArgs {
            command_id: Some(self.command_id()),

            command: Some(GetCommand { key }),
        };

        call!(self, get, args).value
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(key, value, false);
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(key, value, true);
    }
}

impl Clerk {
    fn put_append(&self, key: String, value: String, append: bool) {
        let args = PutAppendArgs {
            command_id: Some(self.command_id()),

            command: Some(PutAppendCommand { key, value, append }),
        };

        _ = call!(self, put_append, args);
    }

    fn command_id(&self) -> CommandId {
        CommandId {
            clerk_id: self.name.clone(),

            clerk_seq: self.sequence.fetch_add(1, Ordering::Relaxed),
        }
    }
}
