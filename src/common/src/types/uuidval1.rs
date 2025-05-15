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

use std::{fmt::{self, Formatter, Write}, mem};

use risingwave_common_estimate_size::EstimateSize;
use uuid::Uuid;
use serde::{Deserialize, Serialize};

use super::{DataType, ScalarImpl, ToText};
 
use crate::types::{Scalar, ScalarRef};


/// A UUID value.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct UuidVal(pub(crate) Box<Uuid>);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct UuidValRef<'a>(pub &'a Uuid);

impl EstimateSize for UuidVal {
    
    fn estimated_heap_size(&self) -> usize {
     mem::size_of::<Uuid>() // Uuid is 16 bytes
     }
    
     fn estimated_size(&self) -> usize {
      self.estimated_heap_size() + mem::size_of::<Self>()
    }
    
}

impl fmt::Display for UuidValRef<'_>  {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.write(f)
    }
}

impl ToText for UuidValRef<'_> {
    fn write<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        self.write(f)
    }
} 
 

impl From<Uuid> for UuidVal {
    fn from(value: Uuid) -> Self {
     Self(Box::new(value))
    }
}
    
impl From<UuidVal> for Uuid {
    fn from(value: UuidVal) -> Self {
        *value.0
    }
}

impl UuidVal {
    #[inline]
    pub fn get_v4() -> Uuid {
        Self(Uuid::new_v4())
    }

     
} 
 
impl Scalar for UuidVal {
    type ScalarRefType<'a> = UuidValRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        UuidValRef(self.0.as_ref())
    }

    fn to_scalar_value(self) -> ScalarImpl {
        // Assuming ScalarImpl can wrap UuidVal
        ScalarImpl::UuidVal(self)
    }
}

impl<'a> ScalarRef<'a> for UuidValRef<'a> {
    type ScalarType = UuidVal;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        UuidVal(Box::new(*self.0))
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::hash::Hash;
        self.0.hash(state)
    }
}

 