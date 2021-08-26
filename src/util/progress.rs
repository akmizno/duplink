use tokio_stream::StreamExt;
use tokio::task;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use std::sync::Arc;

use crate::api::{DupLinkStream, DuplicateStream, UniqueStream};


pub(crate) struct ProgressBar(indicatif::ProgressBar);

pub(crate) struct ProgressBarBuilder {
    length: u64,

    bar_rx: mpsc::Receiver<u64>,

    bar_finished: Arc<Notify>,

    dup_stream: DuplicateStream,
    uniq_stream: UniqueStream,
}

impl ProgressBarBuilder {
    pub(crate) fn new(length: u64, dup_stream: DuplicateStream, uniq_stream: UniqueStream) -> Self {
        let (bar_tx, bar_rx) = mpsc::channel(length as usize);
        let bar_finished = Arc::new(Notify::new());


        let dup_stream = Self::add_dup_stream(dup_stream, length as usize, bar_tx.clone(), bar_finished.clone());
        let uniq_stream = Self::add_uniq_stream(uniq_stream, length as usize, bar_tx.clone(), bar_finished.clone());

        ProgressBarBuilder {
            length,
            bar_rx,
            bar_finished,
            dup_stream,
            uniq_stream,
        }
    }

    pub(crate) fn build(self) -> (ProgressBar, (DuplicateStream, UniqueStream)) {
        let bar = indicatif::ProgressBar::new(self.length);

        // Task for incrementing progress bar.
        {
            let bar = bar.clone();
            let mut bar_rx = self.bar_rx;
            let bar_finished = self.bar_finished.clone();
            task::spawn(async move {
                while let Some(n) = bar_rx.recv().await {
                    bar.inc(n);
                }
                bar.finish_and_clear();
                bar_finished.notify_waiters();
            });
        }

        (ProgressBar(bar), (self.dup_stream, self.uniq_stream))
    }
    fn add_stream_impl<T, F>(mut s: DupLinkStream<T>, max_length: usize, bar_tx: mpsc::Sender<u64>, bar_finished: Arc<Notify>, inc_num: F) -> DupLinkStream<T>
        where T: 'static + Send + std::fmt::Debug,
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
    fn add_uniq_stream(s: UniqueStream, max_length: usize, bar_tx: mpsc::Sender<u64>, bar_finished: Arc<Notify>) -> UniqueStream
    {
        Self::add_stream_impl(s, max_length, bar_tx, bar_finished, |_| 1)
    }
    fn add_dup_stream(s: DuplicateStream, max_length: usize, bar_tx: mpsc::Sender<u64>, bar_finished: Arc<Notify>) -> DuplicateStream
    {
        Self::add_stream_impl(s, max_length, bar_tx, bar_finished, |v| v.len() as u64)
    }
}
