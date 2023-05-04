use atty::Stream;
use compact_rc::Arc;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::task;
use tokio_stream::StreamExt;

use crate::api::{DupLinkStream, DuplicateStream, UniqueStream};

pub(crate) struct ProgressBar(indicatif::ProgressBar);

pub(crate) struct ProgressBarBuilder {
    length: usize,
    enable_bar: bool,
    enable_stream_buffering: bool,
}

impl ProgressBarBuilder {
    pub(crate) fn new() -> Self {
        ProgressBarBuilder {
            length: 0,
            enable_bar: false,
            enable_stream_buffering: true,
        }
    }

    pub(crate) fn length(mut self, len: usize) -> Self {
        self.length = len;
        self
    }

    pub(crate) fn enable_progress_bar(mut self, yes: bool) -> Self {
        self.enable_bar = yes;
        self
    }

    pub(crate) fn enable_stream_buffering(mut self, yes: bool) -> Self {
        self.enable_stream_buffering = yes;
        self
    }

    pub(crate) fn build(
        self,
        dups: DuplicateStream,
        uniqs: UniqueStream,
    ) -> (ProgressBar, (DuplicateStream, UniqueStream)) {
        let stderr_is_tty = atty::is(Stream::Stderr);
        let show_progress = self.enable_bar && stderr_is_tty;

        if show_progress {
            if self.enable_stream_buffering {
                self.build_with_buffered_streams(dups, uniqs)
            } else {
                self.build_with_async_streams(dups, uniqs)
            }
        } else {
            self.build_hidden(dups, uniqs)
        }
    }

    pub(crate) fn build_hidden(
        self,
        dups: DuplicateStream,
        uniqs: UniqueStream,
    ) -> (ProgressBar, (DuplicateStream, UniqueStream)) {
        (ProgressBar(indicatif::ProgressBar::hidden()), (dups, uniqs))
    }

    pub(crate) fn build_with_async_streams(
        self,
        dups: DuplicateStream,
        uniqs: UniqueStream,
    ) -> (ProgressBar, (DuplicateStream, UniqueStream)) {
        //
        let (bar_tx, bar_rx) = mpsc::channel(self.length);

        let (bar, bar_finished) = Self::new_progress_bar(self.length as u64, bar_rx);

        let dups =
            Self::connect_dup_stream(dups, self.length, bar_tx.clone(), bar_finished.clone());

        let uniqs = Self::connect_uniq_stream(uniqs, self.length, bar_tx, bar_finished);

        (ProgressBar(bar), (dups, uniqs))
    }

    pub(crate) fn build_with_buffered_streams(
        self,
        dups: DuplicateStream,
        uniqs: UniqueStream,
    ) -> (ProgressBar, (DuplicateStream, UniqueStream)) {
        //
        let (bar_tx, bar_rx) = mpsc::channel(self.length);

        let (bar, bar_finished) = Self::new_progress_bar(self.length as u64, bar_rx);

        let dups = Self::connect_dup_stream_buffered(
            dups,
            self.length,
            bar_tx.clone(),
            bar_finished.clone(),
        );

        let uniqs = Self::connect_uniq_stream_buffered(uniqs, self.length, bar_tx, bar_finished);

        (ProgressBar(bar), (dups, uniqs))
    }

    fn new_progress_bar_impl(
        length: u64,
        mut bar_rx: mpsc::Receiver<u64>,
    ) -> (indicatif::ProgressBar, Arc<Notify>) {
        let bar = indicatif::ProgressBar::new(length);
        let bar_finished = Arc::new(Notify::new());

        // Task for incrementing progress bar.
        {
            let bar = bar.clone();
            let bar_finished = bar_finished.clone();
            task::spawn(async move {
                while let Some(n) = bar_rx.recv().await {
                    bar.inc(n);
                }
                bar.finish_and_clear();
                bar_finished.notify_waiters();
            });
        }

        (bar, bar_finished)
    }
    fn new_progress_bar(
        length: u64,
        bar_rx: mpsc::Receiver<u64>,
    ) -> (indicatif::ProgressBar, Arc<Notify>) {
        Self::new_progress_bar_impl(length, bar_rx)
    }
    fn connect_stream_impl<T, F>(
        mut s: DupLinkStream<T>,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
        inc_num: F,
    ) -> DupLinkStream<T>
    where
        T: 'static + Send + std::fmt::Debug,
        F: 'static + Send + Fn(&T) -> u64,
    {
        let (tx, rx) = mpsc::channel(max_length);

        task::spawn(async move {
            while let Some(item) = s.next().await {
                let n = inc_num(&item);
                bar_tx.send(n).await.unwrap();
                tx.send(item).await.unwrap();
            }
            drop(bar_tx);

            bar_finished.notified().await;
        });

        DupLinkStream::new(rx)
    }
    fn connect_uniq_stream(
        s: UniqueStream,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
    ) -> UniqueStream {
        Self::connect_stream_impl(s, max_length, bar_tx, bar_finished, |_| 1)
    }
    fn connect_dup_stream(
        s: DuplicateStream,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
    ) -> DuplicateStream {
        Self::connect_stream_impl(s, max_length, bar_tx, bar_finished, |v| v.len() as u64)
    }

    fn connect_stream_buffered_impl<T, F>(
        mut s: DupLinkStream<T>,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
        inc_num: F,
    ) -> DupLinkStream<T>
    where
        T: 'static + Send + std::fmt::Debug,
        F: 'static + Send + Fn(&T) -> u64,
    {
        let (tx, rx) = mpsc::channel(max_length);

        task::spawn(async move {
            let mut buf = Vec::new();

            // Store received outputs into the buffer,
            // and increment the progress bar.
            while let Some(item) = s.next().await {
                let n = inc_num(&item);
                buf.push(item);
                bar_tx.send(n).await.unwrap();
            }
            drop(bar_tx);

            // Then, wait for the progress bar.
            bar_finished.notified().await;

            // Send the buffered data.
            for node in buf.into_iter() {
                tx.send(node).await.unwrap();
            }
        });

        DupLinkStream::new(rx)
    }
    fn connect_uniq_stream_buffered(
        s: UniqueStream,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
    ) -> UniqueStream {
        Self::connect_stream_buffered_impl(s, max_length, bar_tx, bar_finished, |_| 1)
    }
    fn connect_dup_stream_buffered(
        s: DuplicateStream,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
    ) -> DuplicateStream {
        Self::connect_stream_buffered_impl(s, max_length, bar_tx, bar_finished, |v| v.len() as u64)
    }
}
