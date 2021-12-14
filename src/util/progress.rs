use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::task;
use tokio_stream::StreamExt;

use crate::api::{DupLinkStream, DuplicateStream, UniqueStream};

pub(crate) struct ProgressBar(indicatif::ProgressBar);

pub(crate) struct ProgressBarBuilder {
    length: usize,
}

impl ProgressBarBuilder {
    pub(crate) fn new(length: usize) -> Self {
        ProgressBarBuilder { length }
    }

    pub(crate) fn build(
        self,
        dups: DuplicateStream,
        uniqs: UniqueStream,
    ) -> (ProgressBar, (DuplicateStream, UniqueStream)) {
        //
        let (bar_tx, bar_rx) = mpsc::channel(self.length);
        let bar_finished = Arc::new(Notify::new());

        let dups = Self::new_dup_stream(dups, self.length, bar_tx.clone(), bar_finished.clone());

        let uniqs = Self::new_uniq_stream(uniqs, self.length, bar_tx, bar_finished.clone());

        let bar = Self::new_progress_bar(self.length as u64, bar_rx, bar_finished);

        (ProgressBar(bar), (dups, uniqs))
    }
    fn new_progress_bar(
        length: u64,
        mut bar_rx: mpsc::Receiver<u64>,
        bar_finished: Arc<Notify>,
    ) -> indicatif::ProgressBar {
        let bar = indicatif::ProgressBar::new(length);

        // Task for incrementing progress bar.
        {
            let bar = bar.clone();
            task::spawn(async move {
                while let Some(n) = bar_rx.recv().await {
                    bar.inc(n);
                }
                bar.finish_and_clear();
                bar_finished.notify_waiters();
            });
        }

        bar
    }
    fn new_stream_impl<T, F>(
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

            // Store received outputs into buffer,
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
    fn new_uniq_stream(
        s: UniqueStream,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
    ) -> UniqueStream {
        Self::new_stream_impl(s, max_length, bar_tx, bar_finished, |_| 1)
    }
    fn new_dup_stream(
        s: DuplicateStream,
        max_length: usize,
        bar_tx: mpsc::Sender<u64>,
        bar_finished: Arc<Notify>,
    ) -> DuplicateStream {
        Self::new_stream_impl(s, max_length, bar_tx, bar_finished, |v| v.len() as u64)
    }
}
