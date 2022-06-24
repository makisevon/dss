use std::{collections::HashMap, time::Duration};

use futures::{executor, stream::FuturesUnordered, StreamExt};
use futures_timer::Delay;

use labrpc::{Error as RpcError, Result as RpcResult, RpcFuture};

use crate::{
    msg::{CommitArgs, GetArgs, PrewriteArgs, TimestampArgs},
    service::{TsoClient, TxnClient},
};

#[derive(Clone)]
pub struct Client {
    tso_client: TsoClient,
    txn_client: TxnClient,

    transaction: Option<Transaction>,
}

#[derive(Clone)]
struct Transaction {
    timestamp: u64,

    write: HashMap<Vec<u8>, Vec<u8>>,
}

#[allow(dead_code)]
impl Client {
    pub fn new(tso_client: TsoClient, txn_client: TxnClient) -> Self {
        Self {
            tso_client,
            txn_client,

            transaction: None,
        }
    }

    pub fn get_timestamp(&self) -> RpcResult<u64> {
        executor::block_on(self.timestamp())
    }

    pub fn begin(&mut self) {
        assert!(self.transaction.is_none());
        self.transaction = Some(Transaction {
            timestamp: self.get_timestamp().unwrap(),

            write: HashMap::new(),
        });
    }

    pub fn get(&self, key: Vec<u8>) -> RpcResult<Vec<u8>> {
        let args = GetArgs {
            start_ts: self.transaction.as_ref().unwrap().timestamp,

            key,
        };

        Ok(executor::block_on(Self::call(|| self.txn_client.get(&args)))?.value)
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        _ = self.transaction.as_mut().unwrap().write.insert(key, value);
    }

    pub fn commit(&mut self) -> RpcResult<bool> {
        let transaction = self.transaction.take().unwrap();
        if transaction.write.is_empty() {
            return Ok(true);
        }

        executor::block_on(async {
            let start_ts = transaction.timestamp;
            let primary_key = transaction.write.keys().next().unwrap().clone();

            #[allow(clippy::needless_collect)]
            let secondary_keys: Vec<_> = transaction.write.keys().skip(1).cloned().collect();

            for (key, value) in transaction.write.into_iter() {
                let args = PrewriteArgs {
                    start_ts,

                    key,
                    value,

                    primary_key: primary_key.clone(),
                };

                if !Self::call(|| self.txn_client.prewrite(&args))
                    .await?
                    .success
                {
                    return Ok(false);
                }
            }

            let commit_ts = self.timestamp().await?;
            let args = CommitArgs::new(start_ts, commit_ts, primary_key, true);

            let result = Self::call(|| self.txn_client.commit(&args)).await;
            if matches!(&result, Err(RpcError::Other(error)) if error == "reqhook")
                || !result?.success
            {
                return Ok(false);
            }

            let commit = |key| async {
                let args = CommitArgs::new(start_ts, commit_ts, key, false);
                _ = Self::call(|| self.txn_client.commit(&args)).await;
            };

            secondary_keys
                .into_iter()
                .map(commit)
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await;

            Ok(true)
        })
    }
}

impl Client {
    async fn timestamp(&self) -> RpcResult<u64> {
        let args = TimestampArgs {};
        Ok(Self::call(|| self.tso_client.get_timestamp(&args))
            .await?
            .timestamp)
    }

    const RETRY_TIMES: usize = 3;
    const BACKOFF_MILLIS: u64 = 50;

    async fn call<T>(rpc: impl Fn() -> RpcFuture<RpcResult<T>>) -> RpcResult<T> {
        let mut reply = rpc().await;
        for i in 0..Self::RETRY_TIMES {
            if reply.is_ok() {
                return reply;
            }

            Delay::new(Duration::from_millis(Self::BACKOFF_MILLIS << i)).await;
            reply = rpc().await;
        }

        reply
    }
}

impl CommitArgs {
    fn new(start_ts: u64, commit_ts: u64, key: Vec<u8>, is_primary: bool) -> Self {
        Self {
            start_ts,
            commit_ts,

            key,
            is_primary,
        }
    }
}
