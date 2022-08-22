use std::{
    mem,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use futures::{
    channel::mpsc::UnboundedSender, executor::ThreadPool, stream::FuturesUnordered, Future,
    FutureExt, SinkExt, StreamExt,
};
use futures_timer::Delay;

use labcodec::Message;
use labrpc::Result as RpcResult;

use crate::{kvraft::server::Size, proto::raftpb::*, EXECUTOR};

#[cfg(test)]
pub mod config;

pub mod errors;
use self::errors::{Error, Result};

pub mod persister;
use self::persister::Persister;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

pub struct Raft {
    current_term: u64,
    voted_for: Option<usize>,

    log: Log,

    commit_index: u64,
    last_applied: u64,

    role: Role,
    last_sign: Instant,

    peers: Vec<RaftClient>,
    me: usize,

    persister: Box<dyn Persister>,
    apply_ch: UnboundedSender<ApplyMsg>,

    executor: ThreadPool,
    weak: Weak<Mutex<Self>>,
}

#[derive(Message, Clone)]
struct Log {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<Entry>,

    #[prost(uint64, tag = "2")]
    offset: u64,
}

#[derive(PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub enum ApplyMsg {
    Command {
        data: Vec<u8>,

        index: u64,
    },

    Snapshot {
        data: Vec<u8>,

        term: u64,
        index: u64,
    },
}

#[derive(Default, Clone)]
pub struct State {
    pub term: u64,

    pub is_leader: bool,
}

impl Node {
    pub fn new(raft: Raft) -> Self {
        let raft = Arc::new(Mutex::new(raft));
        {
            let mut inner = raft.lock().unwrap();
            inner.weak = Arc::downgrade(&raft);
            inner.spawn_follower_daemon();
        }

        Self { raft }
    }

    pub fn kill(&self) {
        let mut raft = self.raft.lock().unwrap();
        _ = raft.apply_ch.close();
        raft.weak = Weak::new();
    }

    pub fn term(&self) -> u64 {
        self.raft.lock().unwrap().current_term
    }

    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().role == Role::Leader
    }

    pub fn get_state(&self) -> State {
        let raft = self.raft.lock().unwrap();
        State {
            term: raft.current_term,

            is_leader: raft.role == Role::Leader,
        }
    }

    pub fn start(&self, command: &impl Message) -> Result<(u64, u64)> {
        let mut raft = self.raft.lock().unwrap();
        if raft.role != Role::Leader {
            return Err(Error::NotLeader);
        }

        let mut data = Vec::new();
        labcodec::encode(command, &mut data).map_err(Error::Encode)?;

        let term = raft.current_term;
        raft.log.push(Entry::new(data, term));

        raft.persister.save_raft_state(raft.state());
        Ok((raft.log.len() - 1, term))
    }

    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        let mut raft = self.raft.lock().unwrap();
        if index <= raft.log.offset {
            return;
        }

        raft.log.offset(index);
        raft.persister
            .save_state_and_snapshot(raft.state(), snapshot.into());
    }

    pub fn cond_install_snapshot(
        &self,
        _last_included_term: u64,
        _last_included_index: u64,
        _snapshot: &[u8],
    ) -> bool {
        true
    }
}

macro_rules! try_follow {
    ($term:expr, $raft:ident, $reply:expr) => {{
        let term = $term;

        let reply = $reply;
        assert!(reply.term == $raft.current_term);

        if term < $raft.current_term {
            return Ok(reply);
        }

        if term > $raft.current_term {
            $raft.transfer(term, Role::Follower);
        }
    }};
}

macro_rules! must_follow {
    ($term:expr, $raft:ident, $reply:expr) => {{
        let term = $term;
        try_follow!(term, $raft, $reply);

        assert!($raft.role != Role::Leader);
        if $raft.role != Role::Follower {
            $raft.transfer(term, Role::Follower);
        } else {
            $raft.last_sign = Instant::now();
        }
    }};
}

#[async_trait::async_trait]
impl RaftService for Node {
    async fn request_vote(&self, args: RequestVoteArgs) -> RpcResult<RequestVoteReply> {
        let mut raft = self.raft.lock().unwrap();
        try_follow!(
            args.term,
            raft,
            RequestVoteReply::new(raft.current_term, false)
        );

        let candidate_id = args.candidate_id as usize;
        assert!(candidate_id < raft.peers.len());

        let last_term_and_index = (raft.log.last().term, raft.log.len() - 1);
        if matches!(raft.voted_for, Some(id) if id != candidate_id)
            || (args.last_log_term, args.last_log_index) < last_term_and_index
        {
            return Ok(RequestVoteReply::new(raft.current_term, false));
        }

        raft.voted_for = Some(candidate_id);
        raft.last_sign = Instant::now();

        raft.persister.save_raft_state(raft.state());
        Ok(RequestVoteReply::new(raft.current_term, true))
    }

    async fn append_entries(&self, mut args: AppendEntriesArgs) -> RpcResult<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        must_follow!(
            args.term,
            raft,
            AppendEntriesReply::new(raft.current_term, false, 0, 0)
        );

        let len = raft.log.len();
        if args.prev_log_index >= len {
            return Ok(AppendEntriesReply::new(raft.current_term, false, 0, len));
        }

        let prev_entry = raft.log.get(args.prev_log_index);
        if matches!(prev_entry, Some(entry) if entry.term != args.prev_log_term) {
            let term = prev_entry.unwrap().term;
            let last_without = (raft.log.offset..args.prev_log_index)
                .rev()
                .find(|&i| raft.log.get(i).unwrap().term != term)
                .unwrap_or(raft.log.offset);

            return Ok(AppendEntriesReply::new(
                raft.current_term,
                false,
                term,
                last_without + 1,
            ));
        }

        if prev_entry.is_none() {
            args.entries = args
                .entries
                .into_iter()
                .skip((raft.log.offset - args.prev_log_index) as usize)
                .collect();

            args.prev_log_term = raft.log.first().term;
            args.prev_log_index = raft.log.offset;
        }

        let log_offset = args.prev_log_index + 1;
        if let Some((i, _)) = args.entries.iter().enumerate().find(|&(i, entry)| {
            !matches!(raft.log.get(i as u64 + log_offset), Some(last) if last.term == entry.term)
        }) {
            raft.log.truncate(i as u64 + log_offset);
            raft.log.append(&mut args.entries.into_iter().skip(i).collect());
        }

        let last_index = raft.log.len() - 1;
        raft.commit_and_apply(args.leader_commit.min(last_index));

        raft.persister.save_raft_state(raft.state());
        Ok(AppendEntriesReply::new(raft.current_term, true, 0, 0))
    }

    async fn install_snapshot(&self, args: InstallSnapshotArgs) -> RpcResult<InstallSnapshotReply> {
        let mut raft = self.raft.lock().unwrap();
        must_follow!(
            args.term,
            raft,
            InstallSnapshotReply::new(raft.current_term)
        );

        if args.last_included_index <= raft.commit_index {
            return Ok(InstallSnapshotReply::new(raft.current_term));
        }

        assert!(args.last_included_term >= raft.log.get(raft.commit_index).unwrap().term);
        if matches!(
            raft.log.get(args.last_included_index),
            Some(entry) if entry.term == args.last_included_term
        ) {
            raft.log.offset(args.last_included_index);
        } else {
            raft.log
                .reset(args.last_included_term, args.last_included_index);
        }

        raft.snapshot(
            args.data.clone(),
            args.last_included_term,
            args.last_included_index,
        );

        raft.persister
            .save_state_and_snapshot(raft.state(), args.data);

        Ok(InstallSnapshotReply::new(raft.current_term))
    }
}

impl RequestVoteReply {
    fn new(term: u64, vote_granted: bool) -> Self {
        Self { term, vote_granted }
    }
}

impl AppendEntriesReply {
    fn new(term: u64, success: bool, conflicting_term: u64, first_conflicted: u64) -> Self {
        Self {
            term,

            success,

            conflicting_term,
            first_conflicted,
        }
    }
}

impl InstallSnapshotReply {
    fn new(term: u64) -> Self {
        Self { term }
    }
}

impl Size for Node {
    fn size(&self) -> usize {
        self.raft.lock().unwrap().state().len()
    }
}

#[derive(Message)]
struct Persistence {
    #[prost(uint64, tag = "1")]
    current_term: u64,
    #[prost(message, tag = "2")]
    voted_for: Option<u64>,

    #[prost(message, tag = "3")]
    log: Option<Log>,
}

impl Raft {
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Self {
        let mut raft = Self {
            current_term: 0,
            voted_for: None,

            log: Log::new(),

            commit_index: 0,
            last_applied: 0,

            role: Role::Follower,
            last_sign: Instant::now(),

            peers,
            me,

            persister,
            apply_ch,

            executor: ThreadPool::clone(&EXECUTOR),
            weak: Weak::new(),
        };

        let state = raft.persister.raft_state();
        if state.is_empty() {
            return raft;
        }

        let persistence: Persistence = labcodec::decode(&state).unwrap();
        raft.current_term = persistence.current_term;
        raft.voted_for = persistence.voted_for.map(|id| id as usize);

        raft.log = persistence.log.unwrap();
        if raft.log.offset == 0 {
            return raft;
        }

        raft.snapshot(
            raft.persister.snapshot(),
            raft.log.first().term,
            raft.log.offset,
        );

        raft
    }
}

macro_rules! must_upgrade {
    ($weak:ident) => {
        match $weak.upgrade() {
            Some(raft) => raft,
            None => return,
        }
    };
}

macro_rules! must_lock {
    ($raft:ident, $current_term:ident, $role:expr) => {{
        let raft = $raft.lock().unwrap();
        assert!(raft.current_term >= $current_term);

        let role = $role;

        if raft.current_term > $current_term || role == Role::Candidate && raft.role != role {
            return;
        }

        assert!(raft.role == role);
        raft
    }};
}

macro_rules! try_call {
    ($rpc:ident, $timer:ident) => {
        futures::select! {
            reply = $rpc => Some(reply),
            _ = $timer => None,
        }
    };
}

macro_rules! must_unwrap {
    ($reply:ident, $current_term:ident, $raft:ident, $role:expr) => {{
        let reply = match $reply.unwrap() {
            Ok(reply) => reply,
            Err(_) => continue,
        };

        assert!(reply.term >= $current_term);
        if reply.term > $current_term {
            must_lock!($raft, $current_term, $role).transfer(reply.term, Role::Follower);
            return;
        }

        reply
    }};
}

impl Raft {
    fn spawn_follower_daemon(&mut self) {
        self.last_sign = Instant::now();

        let weak = Weak::clone(&self.weak);
        let current_term = self.current_term;

        self.executor.spawn_ok(async move {
            let election_timeout = Self::generate_election_timeout();
            #[allow(clippy::while_let_loop)]
            loop {
                let raft = must_upgrade!(weak);
                let elapsed = {
                    let mut raft = must_lock!(raft, current_term, Role::Follower);
                    let elapsed = raft.last_sign.elapsed();

                    if elapsed > election_timeout {
                        raft.transfer(current_term + 1, Role::Candidate);
                        return;
                    }

                    elapsed
                };

                Delay::new(election_timeout - elapsed).await;
            }
        });
    }

    fn spawn_candidate_daemon(&mut self) {
        self.voted_for = Some(self.me);

        let total = self.peers.len();
        if total == 1 {
            self.transfer(self.current_term, Role::Leader);
            return;
        }

        struct Guard {
            inner: FuturesUnordered<Rpc>,

            executor: ThreadPool,
        }

        type Rpc = Pin<Box<dyn Future<Output = RpcResult<RequestVoteReply>> + Send>>;

        impl Drop for Guard {
            fn drop(&mut self) {
                let rpcs = mem::replace(&mut self.inner, FuturesUnordered::new());
                self.executor
                    .spawn_ok(async { _ = rpcs.collect::<Vec<_>>().await });
            }
        }

        let args = RequestVoteArgs {
            term: self.current_term,

            candidate_id: self.me as u64,

            last_log_term: self.log.last().term,
            last_log_index: self.log.len() - 1,
        };

        let rpcs: FuturesUnordered<_> = self
            .others()
            .map(|(_, peer)| peer.request_vote(&args))
            .collect();

        let mut rpcs = Guard {
            inner: rpcs,

            executor: ThreadPool::clone(&self.executor),
        };

        let weak = Weak::clone(&self.weak);
        let current_term = self.current_term;

        self.executor.spawn_ok(async move {
            let mut election_timer = Delay::new(Self::generate_election_timeout()).fuse();

            let mut votes = 1;
            let majority = total / 2 + 1;

            #[allow(clippy::while_let_loop)]
            loop {
                let raft = must_upgrade!(weak);

                let mut rpc = rpcs.inner.select_next_some();
                let reply = try_call!(rpc, election_timer);

                if reply.is_none() {
                    must_lock!(raft, current_term, Role::Candidate)
                        .transfer(current_term + 1, Role::Candidate);

                    return;
                }

                let reply = must_unwrap!(reply, current_term, raft, Role::Candidate);
                if !reply.vote_granted {
                    continue;
                }

                votes += 1;
                if votes < majority {
                    continue;
                }

                must_lock!(raft, current_term, Role::Candidate)
                    .transfer(current_term, Role::Leader);

                return;
            }
        });
    }

    const HEARTBEAT_PERIOD: Duration = Duration::from_millis(80);

    fn spawn_leader_daemon(&self) {
        let total = self.peers.len();
        if total == 1 {
            return;
        }

        struct State {
            next_index: Vec<u64>,

            match_index: Vec<u64>,
        }

        let state = Arc::new(Mutex::new(State {
            next_index: vec![self.log.len(); total],

            match_index: vec![0; total],
        }));

        let major_index = (total + 1) / 2;

        self.others().for_each(|(id, other)| {
            let weak = Weak::clone(&self.weak);
            let current_term = self.current_term;
            let state = Arc::clone(&state);

            let peer = other.clone();

            other.spawn(async move {
                #[allow(clippy::while_let_loop)]
                loop {
                    let raft = must_upgrade!(weak);

                    enum Rpc {
                        AppendEntries(AppendEntriesArgs),
                        InstallSnapshot(InstallSnapshotArgs),
                    }

                    let (last_matched, rpc) = {
                        let raft = must_lock!(raft, current_term, Role::Leader);

                        let next_index = state.lock().unwrap().next_index[id];
                        if next_index <= raft.log.offset {
                            let args = InstallSnapshotArgs {
                                term: current_term,

                                last_included_term: raft.log.first().term,
                                last_included_index: raft.log.offset,

                                data: raft.persister.snapshot(),
                            };

                            (raft.log.offset, Rpc::InstallSnapshot(args))
                        } else {
                            let prev_log_index = next_index - 1;
                            let args = AppendEntriesArgs {
                                term: current_term,

                                prev_log_term: raft.log.get(prev_log_index).unwrap().term,
                                prev_log_index,

                                entries: raft.log.tail(next_index).cloned().collect(),
                                leader_commit: raft.commit_index,
                            };

                            (raft.log.len() - 1, Rpc::AppendEntries(args))
                        }
                    };

                    macro_rules! must_call {
                        ($rpc:expr, $peer:ident, $current_term:ident, $raft:ident) => {{
                            let mut rpc = $rpc.fuse();
                            let mut heartbeat_timer = Delay::new(Self::HEARTBEAT_PERIOD).fuse();

                            let reply = try_call!(rpc, heartbeat_timer);
                            if reply.is_none() {
                                $peer.spawn(async { _ = rpc.await });
                                continue;
                            }

                            must_unwrap!(reply, $current_term, $raft, Role::Leader)
                        }};
                    }

                    if let Rpc::InstallSnapshot(args) = rpc {
                        _ = must_call!(peer.install_snapshot(&args), peer, current_term, raft);
                        {
                            let mut state = state.lock().unwrap();
                            state.next_index[id] = last_matched + 1;
                        }

                        Delay::new(Self::HEARTBEAT_PERIOD).await;
                        continue;
                    }

                    let args = match rpc {
                        Rpc::AppendEntries(args) => args,
                        Rpc::InstallSnapshot(_) => unreachable!(),
                    };

                    let reply = must_call!(peer.append_entries(&args), peer, current_term, raft);
                    {
                        let mut raft = must_lock!(raft, current_term, Role::Leader);
                        let mut state = state.lock().unwrap();

                        if !reply.success {
                            state.next_index[id] = raft
                                .log
                                .back(reply.conflicting_term)
                                .unwrap_or_else(|| reply.first_conflicted.max(1))
                                .min((state.next_index[id] - 1).max(1));
                        } else if last_matched > state.match_index[id] {
                            state.next_index[id] = last_matched + 1;
                            state.match_index[id] = last_matched;

                            let mut match_index = state.match_index.clone();
                            match_index.sort_unstable();

                            let major_matched = match_index[major_index];
                            if matches!(
                                raft.log.get(major_matched),
                                Some(entry) if entry.term == current_term
                            ) {
                                raft.commit_and_apply(major_matched);
                            }
                        } else {
                            state.next_index[id] = last_matched + 1;
                        }
                    }

                    Delay::new(Self::HEARTBEAT_PERIOD).await;
                }
            });
        });
    }

    fn transfer(&mut self, term: u64, role: Role) {
        assert!(term >= self.current_term);
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }

        self.role = role;
        match self.role {
            Role::Follower => self.spawn_follower_daemon(),
            Role::Candidate => self.spawn_candidate_daemon(),
            Role::Leader => self.spawn_leader_daemon(),
        }

        self.persister.save_raft_state(self.state());
    }

    fn generate_election_timeout() -> Duration {
        Duration::from_millis(240 + rand::random::<u64>() % 240)
    }

    fn others(&self) -> impl Iterator<Item = (usize, &RaftClient)> {
        self.peers
            .iter()
            .enumerate()
            .filter(move |&(id, _)| id != self.me)
    }

    fn commit_and_apply(&mut self, to: u64) {
        assert!(to < self.log.len());
        if to > self.commit_index {
            self.commit_index = to;
        }

        if to <= self.last_applied {
            return;
        }

        (self.last_applied + 1..=to).for_each(|i| {
            _ = self.apply_ch.unbounded_send(ApplyMsg::Command {
                data: self.log.get(i).unwrap().data.clone(),

                index: i,
            });
        });

        self.last_applied = to;
    }

    fn snapshot(&mut self, data: Vec<u8>, term: u64, index: u64) {
        assert!(index > self.commit_index);
        _ = self
            .apply_ch
            .unbounded_send(ApplyMsg::Snapshot { data, term, index });

        self.commit_index = index;
        self.last_applied = index;
    }

    fn state(&self) -> Vec<u8> {
        let persistence = Persistence {
            current_term: self.current_term,
            voted_for: self.voted_for.map(|id| id as u64),

            log: Some(self.log.clone()),
        };

        let mut state = Vec::new();
        labcodec::encode(&persistence, &mut state).unwrap();

        state
    }
}

impl Log {
    fn new() -> Self {
        Self {
            entries: vec![Entry::default()],

            offset: 0,
        }
    }

    fn len(&self) -> u64 {
        self.entries.len() as u64 + self.offset
    }

    fn first(&self) -> &Entry {
        &self.entries[0]
    }

    fn last(&self) -> &Entry {
        self.entries.last().unwrap()
    }

    fn get(&self, index: u64) -> Option<&Entry> {
        self.entries.get(index.checked_sub(self.offset)? as usize)
    }

    fn push(&mut self, entry: Entry) {
        assert!(entry.term >= self.last().term);
        self.entries.push(entry);
    }

    fn append(&mut self, other: &mut Vec<Entry>) {
        if other.is_empty() {
            return;
        }

        assert!(other[0].term >= self.last().term);
        self.entries.append(other)
    }

    fn truncate(&mut self, len: u64) {
        assert!(len > self.offset);
        self.entries.truncate((len - self.offset) as usize);
    }

    fn tail(&self, from: u64) -> impl Iterator<Item = &Entry> {
        assert!(from > self.offset);
        self.entries.iter().skip((from - self.offset) as usize)
    }

    fn back(&self, with: u64) -> Option<u64> {
        let next = self
            .entries
            .binary_search_by_key(&(with + 1), |entry| entry.term)
            .unwrap_or_else(|index| index);

        if next <= 1 {
            return None;
        }

        let index = next - 1;
        (self.entries[index].term == with).then(|| index as u64 + self.offset)
    }

    fn offset(&mut self, to: u64) {
        assert!((self.offset + 1..self.len()).contains(&to));
        self.entries.drain(0..(to - self.offset) as usize);

        self.entries[0].data.clear();
        self.offset = to;
    }

    fn reset(&mut self, term: u64, index: u64) {
        assert!(term >= self.first().term);
        self.entries = vec![Entry::new(Vec::new(), term)];

        assert!(index > self.offset);
        self.offset = index;
    }
}

impl Entry {
    fn new(data: Vec<u8>, term: u64) -> Self {
        Self { data, term }
    }
}

impl State {
    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}
