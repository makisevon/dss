use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver},
        oneshot::{self, Sender},
    },
    executor::ThreadPool,
    FutureExt, StreamExt,
};
use futures_timer::Delay;

use labrpc::{Error as RpcError, Result as RpcResult};

use crate::proto::kvraftpb::{
    CommandId, GetArgs, GetCommand, GetReply, KvService, PutAppendArgs, PutAppendCommand,
    PutAppendReply,
};

use crate::{
    proto::raftpb::RaftClient,
    raft::{errors::Error as RaftError, persister::Persister, ApplyMsg, Node as RaftNode, Raft},
    EXECUTOR,
};

#[derive(Clone)]
pub struct Node {
    server: Arc<KvServer>,
}

pub struct KvServer {
    pub rf: RaftNode,
    state: Option<State>,

    executor: ThreadPool,
    replyer: Arc<Mutex<HashMap<u64, Replyer>>>,
}

struct State {
    store: HashMap<String, String>,
    clerk_seq: HashMap<String, u64>,

    max_raft_state: Option<usize>,
    apply_ch: UnboundedReceiver<ApplyMsg>,
}

struct Replyer {
    term: u64,

    reply_tx: Sender<Option<String>>,
}

impl Node {
    pub fn new(mut server: KvServer) -> Self {
        server.spawn_daemon();
        Self {
            server: Arc::new(server),
        }
    }

    pub fn kill(&self) {
        self.server.rf.kill();
    }

    pub fn is_leader(&self) -> bool {
        self.server.rf.is_leader()
    }
}

#[derive(Message)]
struct IdentCommand {
    #[prost(message, optional, tag = "1")]
    command_id: Option<CommandId>,

    #[prost(oneof = "Command", tags = "2, 3")]
    command: Option<Command>,
}

#[derive(Oneof)]
enum Command {
    #[prost(message, tag = "2")]
    Get(GetCommand),
    #[prost(message, tag = "3")]
    PutAppend(PutAppendCommand),
}

impl Node {
    const REPLY_TIMEOUT: Duration = Duration::from_millis(500);
}

macro_rules! reply {
    ($self:ident, $command:ident, $args:ident, $reply:expr) => {{
        let result = $self.server.rf.start(&IdentCommand {
            command_id: $args.command_id,

            command: Some(Command::$command($args.command.unwrap())),
        });

        if let Err(error) = result {
            let mut reply = $reply;
            reply.is_leader = error != RaftError::NotLeader;
            reply.error = error.to_string();

            return Ok(reply);
        }

        let (index, term) = result.unwrap();
        let (reply_tx, mut reply_rx) = oneshot::channel();

        let replyer = Replyer { term, reply_tx };

        $self.server.replyer.lock().unwrap().insert(index, replyer);
        let reply = futures::select! {
            reply = reply_rx => reply,
            _ = Delay::new(Self::REPLY_TIMEOUT).fuse() => return Err(RpcError::Timeout),
        };

        match reply {
            Ok(reply) => reply,
            Err(error) => return Err(RpcError::Recv(error)),
        }
    }};
}

#[async_trait::async_trait]
impl KvService for Node {
    async fn get(&self, args: GetArgs) -> RpcResult<GetReply> {
        Ok(GetReply::new(
            true,
            &reply!(self, Get, args, GetReply::new(false, "", "")).unwrap(),
            "",
        ))
    }

    async fn put_append(&self, args: PutAppendArgs) -> RpcResult<PutAppendReply> {
        _ = reply!(self, PutAppend, args, PutAppendReply::new(false, ""));
        Ok(PutAppendReply::new(true, ""))
    }
}

impl GetReply {
    fn new(is_leader: bool, value: &str, error: &str) -> Self {
        Self {
            is_leader,

            value: value.into(),
            error: error.into(),
        }
    }
}

impl PutAppendReply {
    fn new(is_leader: bool, error: &str) -> Self {
        Self {
            is_leader,

            error: error.into(),
        }
    }
}

#[derive(Message)]
struct Snapshot {
    #[prost(map = "string, string", tag = "1")]
    store: HashMap<String, String>,

    #[prost(map = "string, uint64", tag = "2")]
    clerk_seq: HashMap<String, u64>,
}

impl KvServer {
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        max_raft_state: Option<usize>,
    ) -> Self {
        let (apply_tx, apply_rx) = mpsc::unbounded();
        let mut state = State {
            store: HashMap::new(),
            clerk_seq: HashMap::new(),

            max_raft_state,
            apply_ch: apply_rx,
        };

        state.snapshot(&persister.snapshot());
        Self {
            rf: RaftNode::new(Raft::new(peers, me, persister, apply_tx)),
            state: Some(state),

            executor: ThreadPool::clone(&EXECUTOR),
            replyer: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub trait Size {
    fn size(&self) -> usize;
}

impl KvServer {
    fn spawn_daemon(&mut self) {
        let raft = self.rf.clone();
        let mut state = self.state.take().unwrap();

        let replyer = Arc::clone(&self.replyer);

        self.executor.spawn_ok(async move {
            while let Some(message) = state.apply_ch.next().await {
                if let ApplyMsg::Snapshot { data, term, index } = message {
                    let condition = raft.cond_install_snapshot(term, index, &data);
                    assert!(condition);

                    state.snapshot(&data);
                    continue;
                }

                let (data, index) = match message {
                    ApplyMsg::Command { data, index } => (data, index),
                    ApplyMsg::Snapshot { .. } => unreachable!(),
                };

                let ident_command: IdentCommand = labcodec::decode(&data).unwrap();

                let command_id = ident_command.command_id.unwrap();
                let sequence = state.clerk_seq.entry(command_id.clerk_id).or_default();

                let apply = command_id.clerk_seq > *sequence;
                if apply {
                    *sequence = command_id.clerk_seq;
                }

                let command = ident_command.command.unwrap();
                let reply = if let Command::Get(command) = command {
                    Some(state.store.get(&command.key).cloned().unwrap_or_default())
                } else if apply {
                    let command = match command {
                        Command::Get(_) => unreachable!(),
                        Command::PutAppend(command) => command,
                    };

                    let value = state.store.entry(command.key).or_default();
                    if command.append {
                        value.push_str(&command.value);
                    } else {
                        *value = command.value;
                    }

                    None
                } else {
                    None
                };

                let raft_state = raft.get_state();
                let replyer = replyer.lock().unwrap().remove(&index);

                if raft_state.is_leader()
                    && matches!(&replyer, Some(replyer) if replyer.term == raft_state.term())
                {
                    _ = replyer.unwrap().reply_tx.send(reply);
                }

                if !matches!(state.max_raft_state, Some(size) if size <= raft.size()) {
                    continue;
                }

                let snapshot = Snapshot {
                    store: state.store.clone(),

                    clerk_seq: state.clerk_seq.clone(),
                };

                let mut data = Vec::new();
                labcodec::encode(&snapshot, &mut data).unwrap();

                raft.snapshot(index, &data);
            }
        });
    }
}

impl State {
    fn snapshot(&mut self, data: &[u8]) {
        if data.is_empty() {
            self.store.clear();
            self.clerk_seq.clear();

            return;
        }

        let snapshot: Snapshot = labcodec::decode(data).unwrap();
        self.store = snapshot.store;
        self.clerk_seq = snapshot.clerk_seq;
    }
}
