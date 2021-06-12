use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio::task;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use std::sync::Arc;

pub(crate) struct ProgressPipeBuilder {
    length: u64,
    // show: bool,

    bar_tx: mpsc::Sender<u64>,
    bar_rx: mpsc::Receiver<u64>,

    stream_count: usize,
    stream_task_tx: mpsc::Sender<()>,
    stream_task_rx: mpsc::Receiver<()>,

    bar_finished: Arc<Notify>,
}

impl ProgressPipeBuilder {
    pub(crate) fn new(length: u64) -> Self {
        let (bar_tx, bar_rx) = mpsc::channel(length as usize);
        let (stream_task_tx, stream_task_rx) = mpsc::channel(1);
        let bar_finished = Arc::new(Notify::new());

        ProgressPipeBuilder {
            length,
            bar_tx,
            bar_rx,
            stream_count: 0,
            stream_task_tx,
            stream_task_rx,
            bar_finished,
        }
    }

    pub(crate) fn build(self) {
        let bar = indicatif::ProgressBar::new(self.length);

        // Task for indicator incrementing.
        {
            let bar = bar.clone();
            let mut bar_rx = self.bar_rx;
            let stream_task_tx = self.stream_task_tx;
            task::spawn(async move {
                while let Some(n) = bar_rx.recv().await {
                    bar.inc(n);
                }
                stream_task_tx.send(()).await.unwrap();
                drop(stream_task_tx);
            });
        }

        // Task for progress bar cleaning.
        {
            let mut stream_task_rx = self.stream_task_rx;
            let bar_finished = self.bar_finished;
            task::spawn(async move {
                while let Some(_) = stream_task_rx.recv().await {}
                bar.finish_and_clear();
                bar_finished.notify_waiters();
            });
        }
    }
    fn add_stream_impl<T, F>(&mut self, mut s: ReceiverStream<T>, inc_num: F) -> ReceiverStream<T>
        where T: 'static + Send + std::fmt::Debug,
              F: 'static + Send + Fn(&T) -> u64,
    {
        self.stream_count += 1;

        let (tx, rx) = mpsc::channel(self.length as usize);

        let bar_tx = self.bar_tx.clone();
        let stream_task_tx = self.stream_task_tx.clone();
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

            // Notify that the stream is ended.
            stream_task_tx.send(()).await.unwrap();
            drop(stream_task_tx);

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
