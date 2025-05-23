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

use risingwave_common::catalog::{Field, SYS_CATALOG_START_ID};
use risingwave_pb::catalog::PbView;
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;

use super::{DatabaseId, OwnedByUserCatalog, SchemaId, ViewId};
use crate::WithOptions;
use crate::user::UserId;

#[derive(Clone, Debug)]
pub struct ViewCatalog {
    pub id: ViewId,
    pub name: String,
    pub schema_id: SchemaId,
    pub database_id: DatabaseId,

    pub owner: UserId,
    pub properties: WithOptions,
    pub sql: String,
    pub columns: Vec<Field>,
}

impl From<&PbView> for ViewCatalog {
    fn from(view: &PbView) -> Self {
        ViewCatalog {
            id: view.id,
            name: view.name.clone(),
            schema_id: view.schema_id,
            database_id: view.database_id,
            owner: view.owner,
            properties: WithOptions::new_with_options(view.properties.clone()),
            sql: view.sql.clone(),
            columns: view.columns.iter().map(|f| f.into()).collect(),
        }
    }
}

impl ViewCatalog {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn with_id(mut self, id: ViewId) -> Self {
        self.id = id;
        self
    }

    /// Returns the SQL statement that can be used to create this view.
    pub fn create_sql(&self, schema: String) -> String {
        if schema == "public" {
            format!("CREATE VIEW {} AS {}", self.name, self.sql)
        } else {
            format!("CREATE VIEW {}.{} AS {}", schema, self.name, self.sql)
        }
    }

    /// Returns the parsed SQL definition when the view was created (`CREATE VIEW` not included).
    ///
    /// Returns error if it's invalid.
    pub fn sql_ast(&self) -> crate::error::Result<Statement> {
        Ok(Parser::parse_exactly_one(&self.sql)?)
    }

    /// Returns true if this view is a system view.
    pub fn is_system_view(&self) -> bool {
        self.id >= SYS_CATALOG_START_ID as u32
    }
}

impl OwnedByUserCatalog for ViewCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
