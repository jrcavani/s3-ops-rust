use anyhow::Result;
use clap::Parser;
use kdam::TqdmParallelIterator;
use log::info;
use rayon::prelude::*;
use s3_ops::sync_wrapper;
use std::io::{BufWriter, Write};

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

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();

    let client = sync_wrapper::AWSSyncClient::new(args.region, args.endpoint_url);

    // create output dir
    std::fs::create_dir_all(format!("{}", &args.bucket))?;

    // Define the prefixes to process in parallel
    generate_hex_strings()
        .into_par_iter()
        .tqdm()
        .try_for_each(|prefix| {
            info!("Prefix {}: Processing...", prefix);

            let (objects, _) = client.list_objects_v2(&args.bucket, &prefix)?;
            info!("Prefix {}: Found {} objects.", prefix, objects.len());
            if objects.len() == 0 {
                return Ok(());
            }
            // write to text file
            let mut writer = BufWriter::new(std::fs::File::create(format!(
                "{}/{}.txt",
                args.bucket, prefix
            ))?);
            writer.write_all("key,size,timestamp\n".as_bytes())?;
            for object in objects {
                writer.write_all(
                    format!("{},{},{}\n", object.key, object.size, object.timestamp).as_bytes(),
                )?;
            }
            writer.flush()?;
            Ok::<(), anyhow::Error>(())
        })?;

    Ok(())
}
