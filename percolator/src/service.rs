use crate::msg::*;

labrpc::service! {
    service timestamp {
        rpc get_timestamp(TimestampArgs) returns (TimestampReply);
    }
}

pub use self::timestamp::{
    add_service as add_tso_service, Client as TsoClient, Service as TsoService,
};

labrpc::service! {
    service transaction {
        rpc get(GetArgs) returns (GetReply);
        rpc prewrite(PrewriteArgs) returns (PrewriteReply);
        rpc commit(CommitArgs) returns (CommitReply);
    }
}

pub use self::transaction::{
    add_service as add_transaction_service, Client as TxnClient, Service as TxnService,
};
