#![cfg_attr(windows, feature(windows_by_handle))]
use std::path::PathBuf;
use tokio_stream::StreamExt;
use clap::{Arg, App, ArgGroup, ArgMatches, crate_version, AppSettings};
use clap::value_t_or_exit;
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

fn build_semaphore(matches: &ArgMatches) -> (util::semaphore::SmallSemaphore, util::semaphore::LargeSemaphore) {
    let builder = util::semaphore::SemaphoreBuilder::new()
        .large_concurrency(!matches.is_present("hdd"));
    let builder = if matches.is_present("fdlimit") {
        builder.max_concurrency(Some(value_t_or_exit!(matches, "fdlimit", usize)))
    } else {
        builder.max_concurrency(None)
    };
    builder.build()
}

fn build_walker(matches: &ArgMatches) -> walk::DirWalker {
    let walker = walk::DirWalker::new()
        .follow_links(!matches.is_present("no-follow"));

    let walker = if matches.is_present("min-depth") {
        walker.min_depth(Some(value_t_or_exit!(matches, "min-depth", usize)))
    } else {
        walker.min_depth(None)
    };

    let walker = if matches.is_present("max-depth") {
        walker.max_depth(Some(value_t_or_exit!(matches, "max-depth", usize)))
    } else {
        walker.max_depth(None)
    };

    walker
}

#[tokio::main]
async fn main() {
    env_logger::init();
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
            .help("Directories or files that duplink will search into."))

        .arg(Arg::with_name("hdd")
            .long("hdd")
            .required(false)
            .takes_value(false)
            .help("Optimize accessing pattern for HDDs.")
            .long_help("Optimize access patterns for HDDs. As a default, duplink optimized for SSDs. With --hdd, duplink will not open large files simultaneously to reduce random reads."))
        .arg(Arg::with_name("fdlimit")
            .long("fdlimit")
            .short("n")
            .required(false)
            .takes_value(true)
            .help("Limit number of open files at a same time"))

        .group(ArgGroup::with_name("follow-group")
            .args(&["follow", "no-follow"])
            .required(false))
        .arg(Arg::with_name("follow")
            .long("follow")
            .short("f")
            .required(false)
            .help("Follow symbolic links. Enabled by default."))
        .arg(Arg::with_name("no-follow")
            .long("no-follow")
            .short("F")
            .required(false)
            .help("Do not follow symbolic links."))

        .arg(Arg::with_name("min-depth")
            .long("min-depth")
            .takes_value(true)
            .required(false)
            .help("Minimum level of directory traversal."))
        .arg(Arg::with_name("max-depth")
            .long("max-depth")
            .takes_value(true)
            .required(false)
            .help("Maximum level of directory traversal."))

        .get_matches();

    let paths: Vec<PathBuf> = matches.values_of("PATH").unwrap()
        .into_iter()
        .map(PathBuf::from)
        .collect_vec();

    let (sem_small, sem_large) = build_semaphore(&matches);

    let nodes = build_walker(&matches)
        .walk(&paths)
        .collect().await;

    let duplink = api::DupLink::new(sem_small, sem_large);
    let (mut dups, mut uniqs) = duplink.find_dups(nodes);

    tokio::task::spawn(async move {
        while let Some(_) = uniqs.next().await {
        }
    });

    let mut out = io::stdout();
    while let Some(dup_nodes) = dups.next().await {
        for node in dup_nodes {
            out.write(format!("{}\n", node.path().display()).as_bytes()).await.unwrap();
        }
        out.write("\n".as_bytes()).await.unwrap();
    }
}
