pub mod raftpb {
    include!(concat!(env!("OUT_DIR"), "/raftpb.rs"));

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
            rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);
            rpc install_snapshot(InstallSnapshotArgs) returns (InstallSnapshotReply);
        }
    }

    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    include!(concat!(env!("OUT_DIR"), "/kvraftpb.rs"));

    labrpc::service! {
        service kv {
            rpc get(GetArgs) returns (GetReply);
            rpc put_append(PutAppendArgs) returns (PutAppendReply);
        }
    }

    pub use self::kv::{add_service as add_kv_service, Client as KvClient, Service as KvService};
}
