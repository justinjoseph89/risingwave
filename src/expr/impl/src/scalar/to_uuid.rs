// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::{Int256Ref, Uuid};
use risingwave_expr::function;

/// Generate a random UUID using the gen_random_uuid() function
#[function("gen_random_uuid() -> uuid")]
pub fn gen_random_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Generate a UUID from
#[function("gen_uuid_from_string(varchar) -> uuid")]
pub fn gen_uuid_from_string(s: &str) -> Uuid {
    Uuid::from_varchar(s)
}

#[function("gen_uuid_from_u256(int256) -> uuid")]
pub fn gen_uuid_from_u256(v: Int256Ref<'_>) -> Uuid {
    // Extract the lower 128 bits from the Int256
    let (_, low) = v.0.into_words();
    
    // Convert the low 128 bits to a u128 and use it to create a UUID
    let low_u128 = low as u128;
    Uuid::from_u128(low_u128)
}

/// Generate a UUID from 16 bytes
#[function("gen_uuid_from_bytea(bytea) -> uuid")]
pub fn gen_uuid_from_bytea(bytes: &[u8]) -> Uuid {
    if bytes.len() == 16 {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Uuid::from_bytes(arr)
    } else {
        // a nil UUID than to panic
        Uuid::nil()
    }
}
