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

use std::sync::Arc;

use iggy_connector_sdk::transforms::{
    Transform, TransformType, add_fields::AddFields, delete_fields::DeleteFields,
};
use serde::de::DeserializeOwned;
use tracing::error;

use crate::{InitialTransformConfig, RuntimeError, TransformsConfig};

pub fn load(config: TransformsConfig) -> Result<Vec<Arc<dyn Transform>>, RuntimeError> {
    let mut transforms: Vec<Arc<dyn Transform>> = vec![];
    let mut types = vec![];
    for (r#type, transform_config) in config.transforms {
        let initial_config =
            serde_json::from_value::<InitialTransformConfig>(transform_config.clone())
                .unwrap_or_default();
        if !initial_config.enabled {
            continue;
        }

        let transform = load_transform(r#type, transform_config)?;
        types.push(r#type);
        transforms.push(transform);
    }

    Ok(transforms)
}

fn load_transform(
    r#type: TransformType,
    config: serde_json::Value,
) -> Result<Arc<dyn Transform>, RuntimeError> {
    Ok(match r#type {
        TransformType::AddFields => Arc::new(AddFields::new(as_config(r#type, config)?)),
        TransformType::DeleteFields => Arc::new(DeleteFields::new(as_config(r#type, config)?)),
    })
}

fn as_config<T: DeserializeOwned>(
    r#type: TransformType,
    config: serde_json::Value,
) -> Result<T, RuntimeError> {
    serde_json::from_value::<T>(config).map_err(|error| {
        error!("Failed to deserialize config for transform: {type}. {error}");
        RuntimeError::InvalidConfig
    })
}
