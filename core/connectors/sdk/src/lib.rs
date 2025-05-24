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

use async_trait::async_trait;
use decoders::{json::JsonStreamDecoder, raw::RawStreamDecoder, text::TextStreamDecoder};
use encoders::{json::JsonStreamEncoder, raw::RawStreamEncoder, text::TextStreamEncoder};
use iggy::prelude::{HeaderKey, HeaderValue};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use strum_macros::{Display, IntoStaticStr};
use thiserror::Error;
use tokio::runtime::Runtime;

pub mod decoders;
pub mod encoders;
pub mod sink;
pub mod source;
pub mod transforms;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

#[async_trait]
pub trait Source: Send + Sync {
    async fn open(&mut self) -> Result<(), Error>;
    async fn poll(&self) -> Result<ProducedMessages, Error>;
    async fn close(&mut self);
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn open(&mut self) -> Result<(), Error>;
    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error>;
    async fn close(&mut self);
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Payload {
    Json(simd_json::OwnedValue),
    Raw(Vec<u8>),
    Text(String),
}

impl Payload {
    pub fn try_into_vec(self) -> Result<Vec<u8>, Error> {
        match self {
            Payload::Json(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            Payload::Raw(value) => Ok(value),
            Payload::Text(text) => Ok(text.into_bytes()),
        }
    }
}

impl std::fmt::Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::Json(value) => write!(
                f,
                "Json({})",
                simd_json::to_string_pretty(value).unwrap_or_default()
            ),
            Payload::Raw(value) => write!(f, "Raw({:#?})", value),
            Payload::Text(text) => write!(f, "Text({})", text),
        }
    }
}

#[repr(C)]
#[derive(
    Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Display, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
pub enum Schema {
    #[strum(to_string = "json")]
    Json,
    #[strum(to_string = "raw")]
    Raw,
    #[strum(to_string = "text")]
    Text,
}

impl Schema {
    pub fn try_into_payload(self, mut value: Vec<u8>) -> Result<Payload, Error> {
        match self {
            Schema::Json => Ok(Payload::Json(
                simd_json::to_owned_value(&mut value).map_err(|_| Error::InvalidJsonPayload)?,
            )),
            Schema::Raw => Ok(Payload::Raw(value)),
            Schema::Text => Ok(Payload::Text(
                String::from_utf8(value).map_err(|_| Error::InvalidTextPayload)?,
            )),
        }
    }

    pub fn decoder(self) -> Arc<dyn StreamDecoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamDecoder),
            Schema::Raw => Arc::new(RawStreamDecoder),
            Schema::Text => Arc::new(TextStreamDecoder),
        }
    }

    pub fn encoder(self) -> Arc<dyn StreamEncoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamEncoder),
            Schema::Raw => Arc::new(RawStreamEncoder),
            Schema::Text => Arc::new(TextStreamEncoder),
        }
    }
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub stream: String,
    pub topic: String,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessagesMetadata {
    pub partition_id: u32,
    pub current_offset: u64,
    pub schema: Schema,
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedMessage {
    pub id: u128,
    pub offset: u64,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProducedMessages {
    pub schema: Schema,
    pub messages: Vec<ProducedMessage>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProducedMessage {
    pub id: Option<u128>,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DecodedMessage {
    pub id: Option<u128>,
    pub offset: Option<u64>,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Payload,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RawMessages {
    pub schema: Schema,
    pub messages: Vec<RawMessage>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RawMessage {
    pub offset: u64,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumedMessage {
    pub offset: u64,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Payload,
}

pub trait StreamDecoder: Send + Sync {
    fn schema(&self) -> Schema;
    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error>;
}

pub trait StreamEncoder: Send + Sync {
    fn schema(&self) -> Schema;
    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum Error {
    #[error("Invalid config")]
    InvalidConfig,
    #[error("Invalid record")]
    InvalidRecord,
    #[error("Invalid transformer")]
    InvalidTransformer,
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(String),
    #[error("Init error: {0}")]
    InitError(String),
    #[error("Invalid payload type")]
    InvalidPayloadType,
    #[error("Invalid JSON payload.")]
    InvalidJsonPayload,
    #[error("Invalid text payload.")]
    InvalidTextPayload,
    #[error("Cannot decode schema {0}")]
    CannotDecode(Schema),
}
