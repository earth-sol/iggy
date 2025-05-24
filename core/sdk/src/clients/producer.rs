use super::send_mode::{BackpressureMode, SendMode};
/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use super::{MAX_BATCH_LENGTH, ORDERING};

use bytes::Bytes;
use futures_util::StreamExt;
use iggy_binary_protocol::Client;
use iggy_common::locking::{IggySharedMut, IggySharedMutFn};
use iggy_common::{
    CompressionAlgorithm, DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration,
    IggyError, IggyExpiry, IggyMessage, IggyTimestamp, MaxTopicSize, Partitioner, Partitioning,
};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Interval, sleep};
use tracing::{error, info, trace, warn};
use dashmap::DashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

struct shard {
    tx: flume::Sender<Vec<IggyMessage>>,
    _join_handle: JoinHandle<()>,
}

impl shard {
    fn new(id: usize) -> Self {
        let (tx, rx) = flume::bounded::<Vec<IggyMessage>>(10); // todo добавить размер в конфигурацию
        let handle = tokio::spawn(async move {
            while let Ok(message) = rx.recv_async().await { // todo поменять на match
                
            }
        });
    }
}

pub trait ErrorCallback: Send + Sync + Debug {
    fn call(&self, error: IggyError, messages: Vec<IggyMessage>);
}

unsafe impl Send for IggyProducer {}
unsafe impl Sync for IggyProducer {}

pub struct IggyProducer {
    initialized: bool,
    can_send: Arc<AtomicBool>,
    client: Arc<IggySharedMut<Box<dyn Client>>>,
    stream_id: Arc<Identifier>,
    stream_name: String,
    topic_id: Arc<Identifier>,
    topic_name: String,
    batch_length: Option<usize>,
    partitioning: Option<Arc<Partitioning>>,
    send_mode: Arc<SendMode>,
    encryptor: Option<Arc<EncryptorKind>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    linger_time_micros: u64,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    topic_message_expiry: IggyExpiry,
    topic_max_size: MaxTopicSize,
    default_partitioning: Arc<Partitioning>,
    can_send_immediately: bool,
    last_sent_at: Arc<AtomicU64>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<IggyDuration>,

    _join_handle: Option<JoinHandle<()>>,
    sema: Arc<Semaphore>,
    sender: Option<Arc<flume::Sender<Vec<IggyMessage>>>>,
    error_callback: Option<Arc<dyn ErrorCallback>>,
    shard_number: usize,
    // todo добавить ShardStrategy
}

impl IggyProducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<Box<dyn Client>>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        batch_length: Option<usize>,
        partitioning: Option<Partitioning>,
        encryptor: Option<Arc<EncryptorKind>>,
        partitioner: Option<Arc<dyn Partitioner>>,
        linger_time: Option<IggyDuration>,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        topic_partitions_count: u32,
        topic_replication_factor: Option<u8>,
        topic_message_expiry: IggyExpiry,
        topic_max_size: MaxTopicSize,
        send_retries_count: Option<u32>,
        send_retries_interval: Option<IggyDuration>,
        send_mode: SendMode,
        error_callback: Option<Arc<dyn ErrorCallback>>,
    ) -> Self {
        Self {
            initialized: false,
            client: Arc::new(client),
            can_send: Arc::new(AtomicBool::new(true)),
            stream_id: Arc::new(stream),
            stream_name,
            topic_id: Arc::new(topic),
            topic_name,
            batch_length,
            partitioning: partitioning.map(Arc::new),
            send_mode: Arc::new(send_mode),
            encryptor,
            partitioner,
            linger_time_micros: linger_time.map_or(0, |i| i.as_micros()),
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            topic_partitions_count,
            topic_replication_factor,
            topic_message_expiry,
            topic_max_size,
            default_partitioning: Arc::new(Partitioning::balanced()),
            can_send_immediately: linger_time.is_none(),
            last_sent_at: Arc::new(AtomicU64::new(0)),
            send_retries_count,
            send_retries_interval,
            _join_handle: None,
            sema: Arc::new(Semaphore::new(10)),
            sender: None,
            error_callback,
            shard_number: default_shard_count(),
        }
    }

    pub fn stream(&self) -> &Identifier {
        &self.stream_id
    }

    pub fn topic(&self) -> &Identifier {
        &self.topic_id
    }

    /// Initializes the producer by subscribing to diagnostic events, creating the stream and topic if they do not exist etc.
    ///
    /// Note: This method must be invoked before producing messages.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        if self.initialized {
            return Ok(());
        }

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        info!("Initializing producer for stream: {stream_id} and topic: {topic_id}...");
        self.subscribe_events().await;
        let client = self.client.clone();
        let client = client.read().await;
        if client.get_stream(&stream_id).await?.is_none() {
            if !self.create_stream_if_not_exists {
                error!("Stream does not exist and auto-creation is disabled.");
                return Err(IggyError::StreamNameNotFound(self.stream_name.clone()));
            }

            let (name, id) = match stream_id.kind {
                IdKind::Numeric => (
                    self.stream_name.to_owned(),
                    Some(self.stream_id.get_u32_value()?),
                ),
                IdKind::String => (self.stream_id.get_string_value()?, None),
            };
            info!("Creating stream: {name}");
            client.create_stream(&name, id).await?;
        }

        if client.get_topic(&stream_id, &topic_id).await?.is_none() {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_name.clone(),
                    self.stream_name.clone(),
                ));
            }

            let (name, id) = match self.topic_id.kind {
                IdKind::Numeric => (
                    self.topic_name.to_owned(),
                    Some(self.topic_id.get_u32_value()?),
                ),
                IdKind::String => (self.topic_id.get_string_value()?, None),
            };
            info!("Creating topic: {name} for stream: {}", self.stream_name);
            client
                .create_topic(
                    &self.stream_id,
                    &self.topic_name,
                    self.topic_partitions_count,
                    CompressionAlgorithm::None,
                    self.topic_replication_factor,
                    id,
                    self.topic_message_expiry,
                    self.topic_max_size,
                )
                .await?;
        }

        let (tx, rx) = flume::bounded::<Vec<IggyMessage>>(0); // todo задать какое-то значение
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partitioning = self.partitioning.clone();
        let default_partitioning = self.default_partitioning.clone();
        let partitioner = self.partitioner.clone();
        let client = self.client.clone();
        let send_retries_count = self.send_retries_count.clone();
        let send_retries_interval = self.send_retries_interval.clone();
        let can_send = self.can_send.clone();
        let send_batches = AtomicUsize::new(0);
        let num_shard = self.shard_number;
        // let sema = self.sema.clone();

        let handle = tokio::spawn(async move {
            while let Ok(mut batch) = rx.recv_async().await {
                // let sema = sema.clone();
                // let partitioner = partitioner.clone();
                // let stream_id = stream_id.clone();
                // let topic_id = topic_id.clone();
                // let partitioning = partitioning.clone();
                // let default_partitioning = default_partitioning.clone();
                // let client = client.clone();
                // let can_send = can_send.clone();

                // if round_robin
                let shard_ix = send_batches.fetch_add(1, Ordering::SeqCst) % num_shard;


                let partitioning = get_partitioning(
                    &partitioner,
                    &stream_id,
                    &topic_id,
                    &batch,
                    partitioning.clone(),
                    &partitioning,
                    default_partitioning.clone(),
                )
                .unwrap();
                try_send_messages(
                    client.clone(),
                    send_retries_count,
                    send_retries_interval,
                    can_send.clone(),
                    &stream_id,
                    &topic_id,
                    &partitioning,
                    &mut batch,
                )
                .await
                .unwrap();
                // tokio::spawn(async move {
                //     let _permit = sema.acquire().await.unwrap();
                //     let partitioning = get_partitioning(
                //         &partitioner,
                //         &stream_id,
                //         &topic_id,
                //         &batch,
                //         partitioning.clone(),
                //         &partitioning,
                //         default_partitioning.clone(),
                //     )
                //     .unwrap();
                //     try_send_messages(
                //         client.clone(),
                //         send_retries_count,
                //         send_retries_interval,
                //         can_send.clone(),
                //         &stream_id,
                //         &topic_id,
                //         &partitioning,
                //         &mut batch,
                //     ).await.unwrap();
                // });
            }
        });
        self._join_handle = Some(handle);
        self.sender = Some(Arc::new(tx));
        self.initialized = true;
        info!(
            "Producer has been initialized for stream: {} and topic: {}.",
            self.stream_id.clone(),
            self.topic_id.clone()
        );
        Ok(())
    }

    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let can_send = self.can_send.clone();

        tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Shutdown => {
                        can_send.store(false, ORDERING);
                        warn!("Client has been shutdown");
                    }
                    DiagnosticEvent::Connected => {
                        can_send.store(false, ORDERING);
                        trace!("Connected to the server");
                    }
                    DiagnosticEvent::Disconnected => {
                        can_send.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        can_send.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        can_send.store(false, ORDERING);
                    }
                }
            }
        });
    }

    pub async fn send(&self, messages: Vec<IggyMessage>) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream_id, &self.topic_id, messages, None)
                .await;
        }

        self.send_buffered(
            self.stream_id.clone(),
            self.topic_id.clone(),
            messages,
            None,
        )
        .await
    }

    // todod добавить канал для считывания
    pub async fn send_async(&self, messages: Vec<IggyMessage>) {
        let sender = self.sender.clone();
        sender.unwrap().send_async(messages).await.unwrap();
    }

    pub async fn send_one(&self, message: IggyMessage) -> Result<(), IggyError> {
        self.send(vec![message]).await
    }

    pub async fn send_with_partitioning(
        &self,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream_id, &self.topic_id, messages, partitioning)
                .await;
        }

        self.send_buffered(
            self.stream_id.clone(),
            self.topic_id.clone(),
            messages,
            partitioning,
        )
        .await
    }

    pub async fn send_to(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream_id, &self.topic_id, messages, partitioning)
                .await;
        }

        self.send_buffered(stream, topic, messages, partitioning)
            .await
    }

    // TODO add batch_size
    async fn send_buffered(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        mut messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        self.encrypt_messages(&mut messages)?;
        let default_partitioning = self.default_partitioning.clone();
        let partitioner = self.partitioner.clone();
        let partitioning = get_partitioning(
            &partitioner,
            &stream,
            &topic,
            &messages,
            partitioning,
            &self.partitioning,
            default_partitioning,
        )?;
        let batch_length = self.batch_length.unwrap_or(MAX_BATCH_LENGTH);
        let batches = messages.chunks_mut(batch_length);
        let mut current_batch = 1;
        let batches_count = batches.len();
        for batch in batches {
            if self.linger_time_micros > 0 {
                Self::wait_before_sending(
                    self.linger_time_micros,
                    self.last_sent_at.load(ORDERING),
                )
                .await;
            }

            let messages_count = batch.len();
            trace!(
                "Sending {messages_count} messages ({current_batch}/{batches_count} batch(es))..."
            );
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);

            let client = self.client.clone();
            let send_retries_count = self.send_retries_count.clone();
            let send_retries_interval = self.send_retries_interval.clone();
            let can_send = self.can_send.clone();

            let sender = self.sender.clone();
            let send_mode = self.send_mode.clone();
            try_send_messages_new(
                BackpressureMode::Block,
                sender,
                self.error_callback.clone(),
                send_mode,
                client,
                send_retries_count,
                send_retries_interval,
                can_send,
                &self.stream_id,
                &self.topic_id,
                &partitioning,
                batch,
            )
            .await?;

            trace!("Sent {messages_count} messages ({current_batch}/{batches_count} batch(es)).");
            current_batch += 1;
        }
        Ok(())
    }

    async fn send_immediately(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        mut messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        trace!("No batch size specified, sending messages immediately.");
        self.encrypt_messages(&mut messages)?;
        let default_partitioning = self.default_partitioning.clone();
        let partitioner = self.partitioner.clone();
        let client = self.client.clone();
        let send_retries_count = self.send_retries_count.clone();
        let send_retries_interval = self.send_retries_interval.clone();
        let can_send = self.can_send.clone();

        let partitioning = get_partitioning(
            &partitioner,
            &stream,
            &topic,
            &messages,
            partitioning,
            &self.partitioning,
            default_partitioning,
        )?;
        let batch_length = self.batch_length.unwrap_or(MAX_BATCH_LENGTH);
        if messages.len() <= batch_length {
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            try_send_messages(
                client.clone(),
                send_retries_count,
                send_retries_interval,
                can_send,
                stream,
                topic,
                &partitioning,
                &mut messages,
            )
            .await?;
            return Ok(());
        }

        for batch in messages.chunks_mut(batch_length) {
            let client = client.clone();
            let can_send = can_send.clone();
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            try_send_messages(
                client,
                send_retries_count,
                send_retries_interval,
                can_send,
                stream,
                topic,
                &partitioning,
                batch,
            )
            .await?;
        }
        Ok(())
    }

    async fn wait_before_sending(interval: u64, last_sent_at: u64) {
        if interval == 0 {
            return;
        }

        let now: u64 = IggyTimestamp::now().into();
        let elapsed = now - last_sent_at;
        if elapsed >= interval {
            trace!("No need to wait before sending messages. {now} - {last_sent_at} = {elapsed}");
            return;
        }

        let remaining = interval - elapsed;
        trace!(
            "Waiting for {remaining} microseconds before sending messages... {interval} - {elapsed} = {remaining}"
        );
        sleep(Duration::from_micros(remaining)).await;
    }

    fn encrypt_messages(&self, messages: &mut [IggyMessage]) -> Result<(), IggyError> {
        if let Some(encryptor) = &self.encryptor {
            for message in messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.header.payload_length = message.payload.len() as u32;
            }
        }
        Ok(())
    }
}

fn get_partitioning(
    partitioner: &Option<Arc<dyn Partitioner>>,
    stream: &Identifier,
    topic: &Identifier,
    messages: &[IggyMessage],
    partitioning: Option<Arc<Partitioning>>,
    producer_partitioning: &Option<Arc<Partitioning>>,
    default_partitioning: Arc<Partitioning>,
) -> Result<Arc<Partitioning>, IggyError> {
    if let Some(partitioner) = partitioner {
        trace!("Calculating partition id using custom partitioner.");
        let partition_id = partitioner.calculate_partition_id(stream, topic, messages)?;
        Ok(Arc::new(Partitioning::partition_id(partition_id)))
    } else {
        trace!("Using the provided partitioning.");
        Ok(partitioning.unwrap_or_else(|| {
            producer_partitioning
                .clone()
                .unwrap_or_else(|| default_partitioning.clone())
        }))
    }
}

async fn wait_until_connected(
    can_send: Arc<AtomicBool>,
    max_retries: u32,
    stream: &Identifier,
    topic: &Identifier,
    timer: &mut Option<Interval>,
) -> Result<(), IggyError> {
    let mut retries = 0;
    while !can_send.load(ORDERING) {
        retries += 1;
        if retries > max_retries {
            error!(
                "Failed to send messages to topic: {topic}, stream: {stream} \
                 after {max_retries} retries. Client is disconnected."
            );
            return Err(IggyError::CannotSendMessagesDueToClientDisconnection);
        }

        error!(
            "Trying to send messages to topic: {topic}, stream: {stream} \
             but the client is disconnected. Retrying {retries}/{max_retries}..."
        );

        if let Some(timer) = timer.as_mut() {
            trace!(
                "Waiting for the next retry to send messages to topic: {topic}, \
                 stream: {stream} for disconnected client..."
            );
            timer.tick().await;
        }
    }
    Ok(())
}

async fn send_with_retries(
    client: Arc<IggySharedMut<Box<dyn Client>>>,
    max_retries: u32,
    stream: &Identifier,
    topic: &Identifier,
    partitioning: &Arc<Partitioning>,
    messages: &mut [IggyMessage],
    timer: &mut Option<Interval>,
) -> Result<(), IggyError> {
    let client = client.read().await;
    let mut retries = 0;
    loop {
        match client
            .send_messages(stream, topic, partitioning, messages)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => {
                retries += 1;
                if retries > max_retries {
                    error!(
                        "Failed to send messages to topic: {topic}, stream: {stream} \
                         after {max_retries} retries. {error}."
                    );
                    return Err(error);
                }

                error!(
                    "Failed to send messages to topic: {topic}, stream: {stream}. \
                     {error} Retrying {retries}/{max_retries}..."
                );

                if let Some(t) = timer.as_mut() {
                    trace!(
                        "Waiting for the next retry to send messages to topic: {topic}, \
                         stream: {stream}..."
                    );
                    t.tick().await;
                }
            }
        }
    }
}

async fn try_send_messages(
    client: Arc<IggySharedMut<Box<dyn Client>>>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<IggyDuration>,
    can_send: Arc<AtomicBool>,
    stream: &Identifier,
    topic: &Identifier,
    partitioning: &Arc<Partitioning>,
    messages: &mut [IggyMessage],
) -> Result<(), IggyError> {
    let rw_client = client.read().await;
    let Some(max_retries) = send_retries_count else {
        return rw_client
            .send_messages(stream, topic, partitioning, messages)
            .await;
    };

    if max_retries == 0 {
        return rw_client
            .send_messages(stream, topic, partitioning, messages)
            .await;
    }

    let mut timer = if let Some(interval) = send_retries_interval {
        let mut timer = tokio::time::interval(interval.get_duration());
        timer.tick().await;
        Some(timer)
    } else {
        None
    };

    wait_until_connected(can_send.clone(), max_retries, stream, topic, &mut timer).await?;
    send_with_retries(
        client.clone(),
        max_retries,
        stream,
        topic,
        partitioning,
        messages,
        &mut timer,
    )
    .await
}

async fn try_send_messages_new(
    backpressure_mode: BackpressureMode,
    sender: Option<Arc<flume::Sender<Vec<IggyMessage>>>>,
    error_callback: Option<Arc<dyn ErrorCallback>>,
    send_mode: Arc<SendMode>,
    client: Arc<IggySharedMut<Box<dyn Client>>>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<IggyDuration>,
    can_send: Arc<AtomicBool>,
    stream: &Identifier,
    topic: &Identifier,
    partitioning: &Arc<Partitioning>,
    messages: &mut [IggyMessage],
) -> Result<(), IggyError> {
    match &*send_mode {
        SendMode::Sync => {
            try_send_messages(
                client,
                send_retries_count,
                send_retries_interval,
                can_send,
                stream,
                topic,
                partitioning,
                messages,
            )
            .await
        }

        SendMode::Background(_cfg) => {
            let out: Vec<IggyMessage> = messages.iter_mut().map(std::mem::take).collect();
            let sender = sender.as_ref().expect("init() must set sender").clone();

            match sender.try_send(out) {
                Ok(()) => Ok(()),

                Err(flume::TrySendError::Full(out)) => match backpressure_mode {
                    BackpressureMode::Block => match sender.send_async(out).await {
                        Ok(()) => Ok(()),
                        Err(flume::SendError(out)) => {
                            if let Some(cb) = error_callback {
                                cb.call(IggyError::BackgroundSendError, out);
                            }
                            Err(IggyError::BackgroundSendError)
                        }
                    },

                    BackpressureMode::BlockWithTimeout(t) => {
                        match tokio::time::timeout(t.get_duration(), sender.send_async(out)).await {
                            Ok(Ok(())) => Ok(()),

                            Ok(Err(flume::SendError(out))) => {
                                if let Some(cb) = error_callback {
                                    cb.call(IggyError::BackgroundSendTimeout, out);
                                }
                                Err(IggyError::BackgroundSendTimeout)
                            }

                            Err(_) => {
                                if let Some(cb) = &error_callback {
                                    cb.call(IggyError::BackgroundSendTimeout, vec![]);
                                }
                                Err(IggyError::BackgroundSendTimeout)
                            }
                        }
                    }

                    BackpressureMode::FailImmediately => {
                        if let Some(cb) = &error_callback {
                            cb.call(IggyError::BackgroundSendBufferFull, out);
                        }
                        Err(IggyError::BackgroundSendBufferFull)
                    }
                },

                Err(flume::TrySendError::Disconnected(out)) => {
                    if let Some(cb) = &error_callback {
                        cb.call(IggyError::BackgroundWorkerDisconnected, out);
                    }
                    error!("Background worker has shut down.");
                    Err(IggyError::BackgroundWorkerDisconnected)
                }
            }
        }
    }
}

fn default_shard_count() -> usize {
    let cpus = num_cpus::get();
    cpus.clamp(2, 16)
}

fn default_sharder(msg: &IggyMessage) -> &[u8] {
    if let Some(h) = &msg.user_headers {
        return &h[..h.len().min(16)];
    }

    &msg.payload[..msg.payload.len().min(16)]
}
