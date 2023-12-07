use anyhow::{Context, Result};
use aws_config::timeout::TimeoutConfig;
use aws_config::Region;
use log::{debug, info};

use aws_sdk_s3::Client;

pub struct ObjectInfo {
    pub key: String,
    pub size: usize,
    pub timestamp: String,
}

pub struct CommonPrefixInfo {
    pub prefix: String,
}

#[derive(Clone)]
pub struct AWSAsyncClient {
    pub client: Client,
}

impl AWSAsyncClient {
    pub async fn new(region: Option<String>, endpoint_url: Option<String>) -> Self {
        let mut config = aws_config::from_env();
        if let Some(region) = region {
            config = config.region(Region::new(region))
        }
        if let Some(endpoint_url) = endpoint_url {
            config = config.endpoint_url(endpoint_url);
        }

        let config = config
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_attempt_timeout(std::time::Duration::from_secs(5))
                    .build(),
            )
            .load()
            .await;

        AWSAsyncClient {
            client: Client::new(&config),
        }
    }

    pub async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
        let mut resp = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .prefix(prefix)
            .into_paginator()
            .send();

        let mut objects: Vec<ObjectInfo> = vec![];
        let mut common_prefixes: Vec<CommonPrefixInfo> = vec![];
        while let Some(item) = resp.next().await {
            let item = item?;
            item.contents().into_iter().try_for_each(|object| {
                objects.push(ObjectInfo {
                    key: object.key().context("No such key!")?.into(),
                    size: object.size().context("No such size!")? as usize,
                    timestamp: object
                        .last_modified()
                        .context("No such timestamp!")?
                        .to_string(),
                });
                Ok::<(), anyhow::Error>(())
            })?;
            item.common_prefixes()
                .into_iter()
                .try_for_each(|common_prefix| {
                    common_prefixes.push(CommonPrefixInfo {
                        prefix: common_prefix.prefix().context("No such prefix!")?.into(),
                    });
                    Ok::<(), anyhow::Error>(())
                })?;
        }

        debug!("Found {} objects in bucket.", objects.len());
        if objects.len() > 0 {
            debug!("Listing first {}...", std::cmp::min(10, objects.len()));
        }
        objects.iter().take(10).for_each(|object| {
            debug!("Object: {}", object.key);
        });

        debug!("Found {} common prefixes.", common_prefixes.len());
        if common_prefixes.len() > 0 {
            debug!(
                "Listing first {}...",
                std::cmp::min(10, common_prefixes.len())
            );
        }
        common_prefixes.iter().take(10).for_each(|common_prefix| {
            debug!("Common Prefix: {}", common_prefix.prefix);
        });

        Ok((objects, common_prefixes))
    }
}
