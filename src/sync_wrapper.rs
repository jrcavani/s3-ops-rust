use crate::async_client::{AWSAsyncClient, CommonPrefixInfo, ObjectInfo};
use anyhow::Result;

pub struct AWSSyncClient {
    pub async_client: AWSAsyncClient,
    rt: tokio::runtime::Runtime,
}

impl AWSSyncClient {
    pub fn new(region: Option<String>, endpoint_url: Option<String>) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        AWSSyncClient {
            async_client: rt.block_on(async { AWSAsyncClient::new(region, endpoint_url).await }),
            rt,
        }
    }

    pub fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
        self.rt
            .block_on(async { self.async_client.list_objects_v2(bucket, prefix).await })
    }
}
