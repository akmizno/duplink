use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio::task;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use std::sync::Arc;

pub(crate) struct ProgressBar(indicatif::ProgressBar);

pub(crate) struct ProgressBarBuilder {
    length: u64,

    bar_tx: mpsc::Sender<u64>,
    bar_rx: mpsc::Receiver<u64>,

    stream_count: usize,
    inc_finished: Arc<Notify>,

    bar_finished: Arc<Notify>,
}

impl ProgressBarBuilder {
    pub(crate) fn new(length: u64) -> Self {
        let (bar_tx, bar_rx) = mpsc::channel(length as usize);
        let inc_finished = Arc::new(Notify::new());
        let bar_finished = Arc::new(Notify::new());

        ProgressBarBuilder {
            length,
            bar_tx,
            bar_rx,
            stream_count: 0,
            inc_finished,
            bar_finished,
        }
    }

    pub(crate) fn build(self) -> ProgressBar {
        let bar = indicatif::ProgressBar::new(self.length);

        // Task for incrementing progress bar.
        {
            let bar = bar.clone();
            let mut bar_rx = self.bar_rx;
            let inc_finished = self.inc_finished.clone();
            task::spawn(async move {
                while let Some(n) = bar_rx.recv().await {
                    bar.inc(n);
                }
                inc_finished.notify_waiters();
            });
        }

        // Task for cleaning the progress bar.
        {
            let bar = bar.clone();
            let inc_finished = self.inc_finished;
            let bar_finished = self.bar_finished;
            task::spawn(async move {
                inc_finished.notified().await;
                bar.finish_and_clear();
                bar_finished.notify_waiters();
            });
        }

        ProgressBar(bar)
    }
    fn add_stream_impl<T, F>(&mut self, mut s: ReceiverStream<T>, inc_num: F) -> ReceiverStream<T>
        where T: 'static + Send + std::fmt::Debug,
              F: 'static + Send + Fn(&T) -> u64,
    {
        self.stream_count += 1;

        let (tx, rx) = mpsc::channel(self.length as usize);

        let bar_tx = self.bar_tx.clone();
        let bar_finished = self.bar_finished.clone();
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

        ReceiverStream::new(rx)
    }
    pub(crate) fn add_stream<T>(&mut self, s: ReceiverStream<T>) -> ReceiverStream<T>
        where T: 'static + Send + std::fmt::Debug
    {
        self.add_stream_impl(s, |_| 1)
    }
    pub(crate) fn add_vec_stream<T>(&mut self, s: ReceiverStream<Vec<T>>) -> ReceiverStream<Vec<T>>
        where T: 'static + Send + std::fmt::Debug
    {
        self.add_stream_impl(s, |v| v.len() as u64)
    }
}
