#![cfg_attr(windows, feature(windows_by_handle))]

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::value_t_or_exit;
use clap::{crate_version, App, AppSettings, Arg, ArgGroup, ArgMatches};
use itertools::Itertools;
use std::path::PathBuf;
use tokio_stream::StreamExt;

use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

pub mod api;
pub mod entry;
pub mod find;
pub(crate) mod util;
pub mod walk;

use api::{DuplicateStream, UniqueStream};
use entry::FileAttr;
use util::progress::ProgressBarBuilder;
use walk::Node;

fn write_uniqs(mut out: Output, mut uniqs: UniqueStream) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        while let Some(node) = uniqs.next().await {
            out.write(format!("{}\n", node.path().display()).as_bytes())
                .await
                .unwrap();
        }
        out.flush().await.unwrap();
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
        out.flush().await.unwrap();
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
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Output::Sink(o) => o.flush().await,
            Output::Stdout(o) => o.flush().await,
            Output::File(o) => o.flush().await,
        }
    }

    fn is_stdout(&self) -> bool {
        matches!(self, Output::Stdout(_))
    }
}
impl From<fs::File> for Output {
    fn from(w: fs::File) -> Self {
        Output::File(io::BufWriter::new(w))
    }
}
async fn build_output_writers(
    output_file: Option<PathBuf>,
    unique: bool,
) -> (bool, (Output, Output)) {
    let output = if let Some(opath) = output_file {
        let file = fs::File::create(opath).await.unwrap();
        Output::from(file)
    } else {
        Output::stdout()
    };

    let output_is_stdout = output.is_stdout();

    if unique {
        (output_is_stdout, (Output::sink(), output))
    } else {
        (output_is_stdout, (output, Output::sink()))
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

        .arg(Arg::with_name("PATH")
            .multiple(true)
            .takes_value(true)
            .required(true)
            .help("Directories or files that duplink will search into."))

        .arg(Arg::with_name("output")
            .long("output")
            .short("o")
            .takes_value(true)
            .required(false)
            .help("Output file.")
            .long_help("Output file to write results. (default: stdout)."))

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

        .group(ArgGroup::with_name("ask-group")
            .args(&["ask-each", "yes"])
            .required(false))
        .group(ArgGroup::with_name("hardlink-group")
            .args(&["dedup-hard", "ignore-filesystem"])
            .required(false))
        .group(ArgGroup::with_name("dedup-group")
            .args(&["dedup-hard", "dedup-soft", "unique"])
            .required(false))
        .arg(Arg::with_name("dedup-hard")
            .long("dedup-hard")
            .short("L")
            .required(false)
            .takes_value(false)
            .help("Convert duplicate files to hard links."))
        .arg(Arg::with_name("dedup-soft")
            .long("dedup-soft")
            .short("l")
            .required(false)
            .takes_value(false)
            .help("Convert duplicate files to symbolic links."))
        .arg(Arg::with_name("ask-each")
            .long("ask-each")
            .short("a")
            .required(false)
            .takes_value(false)
            .help("Ask for each detected files.")
            .long_help("Ask for each detected files whether to execute de-duplication. Use with --dedup-hard or --dedup-soft."))
        .arg(Arg::with_name("yes")
            .long("yes")
            .short("y")
            .required(false)
            .takes_value(false)
            .help("De-duplicate all detected files without asking.")
            .long_help("De-duplicate all detected files without asking. Use with --dedup-hard or --dedup-soft."))

        .arg(Arg::with_name("ignore-filesystem")
            .long("ignore-filesystem")
            .required(false)
            .takes_value(false)
            .help("Detect duplicate files across multiple file systems."))

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

        .group(ArgGroup::with_name("progress-group")
            .args(&["progress", "debug"])
            .required(false))
        .group(ArgGroup::with_name("quiet-group")
            .args(&["quiet", "debug"])
            .required(false))
        .arg(Arg::with_name("progress")
            .long("progress")
            .short("p")
            .required(false)
            .help("Show progress bar."))
        .arg(Arg::with_name("quiet")
            .long("quiet")
            .short("q")
            .required(false)
            .help("Disable any message outputs."))
        .arg(Arg::with_name("debug")
            .long("debug")
            .short("d")
            .required(false)
            .help("Show debug messages."))

        .get_matches();

    let enable_progress = matches.is_present("progress");
    let no_msg = enable_progress || matches.is_present("quiet");
    let debug = matches.is_present("debug");

    let log_level = if debug {
        log::LevelFilter::max()
    } else if no_msg {
        log::LevelFilter::Off
    } else {
        log::LevelFilter::Warn
    };

    env_logger::Builder::new()
        .filter_level(log_level)
        .parse_env("DUPLINK_LOG")
        .init();

    let max_depth = if matches.is_present("max-depth") {
        Some(value_t_or_exit!(matches, "max-depth", usize))
    } else {
        None
    };

    let min_depth = if matches.is_present("min-depth") {
        Some(value_t_or_exit!(matches, "min-depth", usize))
    } else {
        None
    };

    let follow = !matches.is_present("no-follow");

    let ignore_dev = matches.is_present("ignore-filesystem");

    let paths: Vec<PathBuf> = matches
        .values_of("PATH")
        .unwrap()
        .into_iter()
        .map(PathBuf::from)
        .collect_vec();

    let (sem_small, sem_large) = build_semaphore(&matches);

    let nodes: Vec<Node> = build_walker(&matches)
        .min_depth(min_depth)
        .max_depth(max_depth)
        .follow_links(follow)
        .walk(&paths)
        .collect()
        .await;
    let nodes_len = nodes.len();

    let dup_finder = api::DupFinder::new(sem_small, sem_large).ignore_dev(ignore_dev);
    let (dups, uniqs) = dup_finder.find_dups(nodes);

    let (output_is_stdout, (dup_out, uniq_out)) = build_output_writers(
        matches.value_of("output").map(PathBuf::from),
        matches.is_present("unique"),
    )
    .await;

    let (_bar, (dups, uniqs)) = ProgressBarBuilder::new()
        .length(nodes_len)
        .enable_progress_bar(enable_progress)
        .enable_stream_buffering(output_is_stdout && atty::is(atty::Stream::Stdout))
        .build(dups, uniqs);

    let uniq_handle = write_uniqs(uniq_out, uniqs);
    let dup_handle = write_dups(dup_out, dups);

    uniq_handle.await.unwrap();
    dup_handle.await.unwrap();
}
