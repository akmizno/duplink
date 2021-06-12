#![cfg_attr(windows, feature(windows_by_handle))]
use clap::value_t_or_exit;
use clap::{crate_version, App, AppSettings, Arg, ArgGroup, ArgMatches};
use itertools::Itertools;
use std::path::PathBuf;
use tokio_stream::StreamExt;

use tokio::fs;
use tokio::io::{self, AsyncWriteExt};
// use std::io::{stdout, Write};
// use std::io::BufWriter;

pub mod api;
pub mod entry;
pub mod find;
pub(crate) mod util;
pub mod walk;

use api::{DuplicateStream, UniqueStream};
use entry::FileAttr;
use util::progress::ProgressPipeBuilder;
use walk::Node;

fn write_uniqs(mut out: Output, mut uniqs: UniqueStream) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        while let Some(node) = uniqs.next().await {
            out.write(format!("{}\n", node.path().display()).as_bytes())
                .await
                .unwrap();
        }
    })
}
fn write_dups(mut out: Output, mut dups: DuplicateStream) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        while let Some(dup_nodes) = dups.next().await {
            for node in dup_nodes {
                out.write(format!("{}\n", node.path().display()).as_bytes())
                    .await
                    .unwrap();
            }
            out.write("\n".as_bytes()).await.unwrap();
        }
    })
}

enum Output {
    Sink(io::Sink),
    Stdout(io::BufWriter<io::Stdout>),
    File(io::BufWriter<fs::File>),
}

impl Output {
    fn stdout() -> Self {
        Output::Stdout(io::BufWriter::new(io::stdout()))
    }
    fn sink() -> Self {
        Output::Sink(io::sink())
    }
    async fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        match self {
            Output::Sink(o) => o.write(src).await,
            Output::Stdout(o) => o.write(src).await,
            Output::File(o) => o.write(src).await,
        }
    }
}
impl From<fs::File> for Output {
    fn from(w: fs::File) -> Self {
        Output::File(io::BufWriter::new(w))
    }
}
fn build_output_writers(matches: &ArgMatches) -> (Output, Output) {
    if matches.is_present("unique") {
        (Output::sink(), Output::stdout())
    } else {
        (Output::stdout(), Output::sink())
    }
}

fn build_semaphore(
    matches: &ArgMatches,
) -> (
    util::semaphore::SmallSemaphore,
    util::semaphore::LargeSemaphore,
) {
    let builder =
        util::semaphore::SemaphoreBuilder::new().large_concurrency(!matches.is_present("hdd"));
    let builder = if matches.is_present("fdlimit") {
        builder.max_concurrency(Some(value_t_or_exit!(matches, "fdlimit", usize)))
    } else {
        builder.max_concurrency(None)
    };
    builder.build()
}

fn build_walker(matches: &ArgMatches) -> walk::DirWalker {
    let walker = walk::DirWalker::new().follow_links(!matches.is_present("no-follow"));

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

        .arg(Arg::with_name("unique")
            .long("unique")
            .takes_value(false)
            .required(false)
            .help("Find unique file instead of duplicates."))

        .group(ArgGroup::with_name("message-group")
            .args(&["progress", "debug", "quiet"])
            .required(false))
        .arg(Arg::with_name("progress")
            .long("progress")
            .short("p")
            .required(false)
            .help("Show progress bar."))
        .arg(Arg::with_name("debug")
            .long("debug")
            .short("d")
            .required(false)
            .help("Show debug messages."))
        .arg(Arg::with_name("quiet")
            .long("quiet")
            .short("q")
            .required(false)
            .help("Disable any message outputs."))

        .get_matches();

    let mut logger_builder = env_logger::Builder::new();
    let logger_builder = if matches.is_present("debug") {
        logger_builder.filter_level(log::LevelFilter::max())
    } else if matches.is_present("quiet") {
        logger_builder.filter_level(log::LevelFilter::Off)
    } else {
        logger_builder.filter_level(log::LevelFilter::Warn)
    };
    logger_builder.parse_env("DUPLINK_LOG")
        .init();

    let paths: Vec<PathBuf> = matches
        .values_of("PATH")
        .unwrap()
        .into_iter()
        .map(PathBuf::from)
        .collect_vec();

    let (sem_small, sem_large) = build_semaphore(&matches);

    let nodes: Vec<Node> = build_walker(&matches).walk(&paths).collect().await;
    let nodes_len = nodes.len();

    let duplink = api::DupLink::new(sem_small, sem_large);
    let (dups, uniqs) = duplink.find_dups(nodes);

    let (dups, uniqs) = if matches.is_present("progress") {
        let mut progress_pipe = ProgressPipeBuilder::new(nodes_len as u64);
        let dups = progress_pipe.add_vec_stream(dups);
        let uniqs = progress_pipe.add_stream(uniqs);
        progress_pipe.build();
        (dups, uniqs)
    } else {
        (dups, uniqs)
    };

    let (dup_out, uniq_out) = build_output_writers(&matches);

    let uniq_handle = write_uniqs(uniq_out, uniqs);
    let dup_handle = write_dups(dup_out, dups);

    uniq_handle.await.unwrap();
    dup_handle.await.unwrap();
}
