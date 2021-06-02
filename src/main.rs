#![cfg_attr(windows, feature(windows_by_handle))]
use std::path::PathBuf;
use tokio_stream::StreamExt;
// use clap::value_t_or_exit;
use clap::{Arg, App, crate_version, AppSettings};
use itertools::Itertools;

use tokio::io::{self, AsyncWriteExt};
// use std::io::{stdout, Write};
// use std::io::BufWriter;

pub mod entry;
pub mod walk;
pub mod find;
pub(crate) mod util;
pub mod api;

use entry::FileAttr;

#[tokio::main]
async fn main() {
    let matches = App::new("duplink")
        .setting(AppSettings::ColoredHelp)
        .author("Akira MIZUNO, akmizno@gmail.com")
        .version(crate_version!())
        .about("A tool for finding duplicate files and de-duplicating them.")
        // .after_help(console::extra_doc().as_ref())

        .arg(Arg::with_name("PATH")
            .multiple(true)
            .takes_value(true)
            .required(true)
            .help("Directories or files to be searched")
        )
        .get_matches();

    let paths: Vec<PathBuf> = matches.values_of("PATH").unwrap()
        .into_iter()
        .map(PathBuf::from)
        .collect_vec();

    let (sem_small, sem_large) = util::semaphore::SemaphoreBuilder::new().large_concurrency(true).build();
    // let (sem_small, sem_large) = util::semaphore::SemaphoreBuilder::new().large_concurrency(false).build();
    let duplink = api::DupLink::new(sem_small, sem_large);
    let nodes = walk::DirWalker::new()
        .walk(&paths)
        .collect().await;
    let (mut dupes, mut uniqs) = duplink.find_dupes(nodes);

    tokio::task::spawn(async move {
        while let Some(_) = uniqs.next().await {
        }
    });

    let mut out = io::stdout();
    while let Some(dup_nodes) = dupes.next().await {
        for node in dup_nodes {
            out.write(format!("{}\n", node.path().display()).as_bytes()).await.unwrap();
        }
        out.write("\n".as_bytes()).await.unwrap();
    }
}
