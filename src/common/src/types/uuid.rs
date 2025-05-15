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

use std::fmt::{Display, Formatter, Write};
use std::hash::Hasher;
use std::io::Read;
use std::mem;
use std::str::FromStr;

use bytes::{BufMut, Bytes, BytesMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::ArrayType;
use serde::{Deserialize, Serialize};
 use to_text::ToText;
use uuid::Timestamp;

use crate::array::ArrayResult;
use crate::types::to_binary::ToBinary;
use crate::types::{Buf, DataType, Scalar, ScalarRef, to_text};

/// A UUID data type (128-bit).
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash, Serialize, Deserialize)]
pub struct Uuid(pub(crate) uuid::Uuid);

/// A reference to a `Uuid` value.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct UuidRef<'a>(pub &'a uuid::Uuid);

impl Display for UuidRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Scalar for Uuid {
    type ScalarRefType<'a> = UuidRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        UuidRef(&self.0)
    }
}

impl<'a> ScalarRef<'a> for UuidRef<'a> {
    type ScalarType = Uuid;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        Uuid(*self.0)
    }

    fn hash_scalar<H: Hasher>(&self, state: &mut H) {
        use std::hash::Hash as _;
        self.0.hash(state)
    }
}

impl FromStr for Uuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::from_str(s).map(Self)
    }
}

impl Uuid {
    /// Create a new UUID from a 16-byte array
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Creates a UUID using the supplied bytes.
    #[inline]
    pub fn from_u128(v: u128) -> Self {
        Self(uuid::Uuid::from_u128(v))
    }

    /// Return the 128-bit value as a u128.
    #[inline]
    pub fn as_u128(&self) -> u128 {
        self.0.as_u128()
    }

    /// Create a new nil (all zeros) UUID.
    #[inline]
    pub fn nil() -> Self {
        Self(uuid::Uuid::nil())
    }

    /// Generate a random UUID (v4).
    #[inline]
    pub fn new_v4() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Create a UUID using a name based on a namespace ID and name (v5).
    #[inline]
    pub fn new_v5(name: Timestamp) -> Self {
        Self(uuid::Uuid::new_v7(name))
    }

    /// Returns the size in bytes of a UUID.
    #[inline]
    pub const fn size() -> usize {
        mem::size_of::<uuid::Uuid>()
    }

    /// Returns the array type for UUIDs in the protocol buffer.
    #[inline]
    pub fn array_type() -> ArrayType {
        ArrayType::Uuid
    }

    /// Creates a UUID from raw bytes of the specified endianness.
    ///#[inline]
    ///pub fn from_ne_bytes(bytes: [u8; 16]) -> Self {
    ///    Self(uuid::Uuid::from_bytes_ne(bytes))
    ///}

    /// Creates a UUID from raw little-endian bytes.
    #[inline]
    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes_le(bytes))
    }

    /// Creates a UUID from raw big-endian bytes.
    #[inline]
    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Deserialize from protocol buffer representation.
    pub fn from_protobuf(input: &mut impl Read) -> ArrayResult<Self> {
        let mut buf = [0u8; 16];
        input.read_exact(&mut buf)?;
        Ok(Self::from_be_bytes(buf))
    }

    /// Parse a UUID from binary representation.
    pub fn from_binary(mut input: &[u8]) -> ArrayResult<Self> {
        let mut buf = [0; 16];
        input.read_exact(&mut buf)?;
        Ok(Self::from_be_bytes(buf))
    }

    /// Deserialize from a memcomparable encoding.
    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let bytes = <[u8; 16]>::deserialize(deserializer)?;
        Ok(Self::from_be_bytes(bytes))
    }
}

impl UuidRef<'_> {
    /// Convert to raw bytes in little-endian order.
    #[inline]
    pub fn to_le_bytes(self) -> [u8; 16] {
        self.0.to_bytes_le()
    }

    /// Convert to raw bytes in big-endian order.
    #[inline]
    pub fn to_be_bytes(self) -> [u8; 16] {
        self.0.as_bytes().clone()
    }

    /// Convert to raw bytes in native-endian order.
    ///#[inline]
    ///pub fn to_ne_bytes(self) -> [u8; 16] {
    ///   self.0.to_bytes_ne()
    ///}

    /// Serialize to protocol buffer representation.
    pub fn to_protobuf<T: std::io::Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }

    /// Serialize to memcomparable format.
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        let bytes = self.to_be_bytes();
        bytes.serialize(serializer)
    }
}

impl ToText for UuidRef<'_> {
    fn write<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        self.write(f)
    }
}

impl ToBinary for UuidRef<'_> {
    fn to_binary_with_type(&self, _ty: &DataType) -> crate::types::to_binary::Result<Bytes> {
        let mut output = BytesMut::new();
        output.put_slice(self.0.as_bytes());
        Ok(output.freeze())
    }
}

impl EstimateSize for Uuid {
    fn estimated_heap_size(&self) -> usize {
        0 // UUIDs are fixed size and don't allocate on the heap
    }
}

// PostgreSQL compatibility

impl ToSql for Uuid {
    accepts!(UUID);

    to_sql_checked!();

    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

impl<'a> FromSql<'a> for Uuid {
    accepts!(UUID);

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("invalid UUID length".into());
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(raw);
        Ok(Self(uuid::Uuid::from_bytes(bytes)))
    }
}

impl<'a> ToSql for UuidRef<'a> {
    accepts!(UUID);

    to_sql_checked!();

    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_from_str() {
        let uuid_str = "123e4567-e89b-12d3-a456-426614174000";
        let uuid = Uuid::from_str(uuid_str).unwrap();
        assert_eq!(uuid.to_string(), uuid_str);
    }

    #[test]
    fn test_uuid_nil() {
        let nil = Uuid::nil();
        assert_eq!(nil.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_uuid_v4() {
        let uuid = Uuid::new_v4();
        assert_ne!(uuid, Uuid::nil());
    }

    #[test]
    fn test_uuid_bytes_conversion() {
        let bytes = [
            0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 
            0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00
        ];
        let uuid = Uuid::from_bytes(bytes);
        assert_eq!(uuid.as_scalar_ref().to_be_bytes(), bytes);
    }

    #[test]
    fn test_uuid_estimate_size() {
        let uuid = Uuid::new_v4();
        assert_eq!(uuid.estimated_heap_size(), 0);
    }
}