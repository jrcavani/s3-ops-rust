use anyhow::Result;
use clap::Parser;
use kdam::TqdmIterator;
use log::info;
use s3_ops::async_client;
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, io::BufWriter, sync::Semaphore};
use tokio_util::task::TaskTracker;

#[derive(Parser)]
struct Cli {
    /// The bucket name
    #[clap(long)]
    bucket: String,
    /// The region
    #[clap(long)]
    region: Option<String>,
    /// The endpoint URL
    #[clap(long)]
    endpoint_url: Option<String>,
}

fn generate_hex_strings() -> Vec<String> {
    let mut result = vec![];

    for i in 0..65536 {
        // Format the number as a 4-digit hexadecimal string
        let hex_string = format!("{:04x}", i);
        result.push(hex_string);
    }
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();

    let client = async_client::AWSAsyncClient::new(args.region, args.endpoint_url).await;

    // create output dir
    tokio::fs::create_dir_all(format!("{}", &args.bucket)).await?;

    let sem = Arc::new(Semaphore::new(32));
    let tracker = TaskTracker::new();

    // Define the prefixes to process in parallel
    for prefix in generate_hex_strings().into_iter().tqdm() {
        info!("Prefix {}: Processing...", prefix);

        let client = client.clone();
        let bucket = args.bucket.clone();
        let permit = Arc::clone(&sem).acquire_owned().await;
        tracker.spawn(async move {
            let _permit = permit; // consume semaphore
            let (objects, _) = client.list_objects_v2(&bucket, &prefix).await?;
            info!("Prefix {}: Found {} objects.", prefix, objects.len());
            if objects.len() == 0 {
                return Ok(());
            }
            // write to text file
            let mut writer = BufWriter::new(
                tokio::fs::File::create(format!("{}/{}.txt", bucket, prefix)).await?,
            );
            writer.write_all("key,size,timestamp\n".as_bytes()).await?;
            for object in objects {
                writer
                    .write_all(
                        format!("{},{},{}\n", object.key, object.size, object.timestamp).as_bytes(),
                    )
                    .await?;
            }
            writer.flush().await?;
            Ok::<(), anyhow::Error>(())
        });
    }

    // wait for all tasks to finish
    tracker.close();
    tracker.wait().await;
    info!("All tasks finished.");

    Ok(())
}
