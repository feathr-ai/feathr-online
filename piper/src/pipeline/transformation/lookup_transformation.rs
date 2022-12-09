use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use futures::future::join_all;

use crate::pipeline::{
    expression::Expression, lookup::LookupSource, Column, DataSet, PiperError, Schema, Value,
    ValueType,
};

use super::Transformation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Single,
    LeftInner,
    LeftOuter,
}

impl Default for JoinKind {
    fn default() -> Self {
        Self::Single
    }
}

#[derive(Debug)]
pub struct LookupTransformation {
    join_kind: JoinKind,
    lookup_source_name: String,
    lookup_source: Arc<dyn LookupSource>,
    key: Arc<dyn Expression>,
    lookup_fields: Schema,
    output_schema: Arc<Schema>,
}

impl LookupTransformation {
    pub fn create(
        join_kind: JoinKind,
        input_schema: &Schema,
        lookup_source_name: String,
        lookup_source: Arc<dyn LookupSource>,
        lookup_fields: Vec<(String, Option<String>, ValueType)>, // (Lookup field, new name, type)
        key: Box<dyn Expression>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let lookup_schema: Schema = lookup_fields
            .iter()
            .map(|(name, _, ty)| Column::new(name.clone(), *ty))
            .collect();
        let rename_map: HashMap<String, String> = lookup_fields
            .iter()
            .filter_map(|(name, new_name, _)| new_name.clone().map(|n| (name.clone(), n)))
            .collect();
        let output_schema = Arc::new(
            input_schema
                .clone()
                .columns
                .into_iter()
                .chain(lookup_fields.into_iter().map(|(name, _, ty)| {
                    Column::new(rename_map.get(&name).unwrap_or(&name).clone(), ty)
                }))
                .collect(),
        );
        Ok(Box::new(Self {
            join_kind,
            lookup_source_name,
            lookup_source,
            key: key.into(),
            lookup_fields: lookup_schema,
            output_schema,
        }))
    }
}

impl Transformation for LookupTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        let lookup_field_names = self
            .lookup_fields
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let lookup_field_types = self
            .lookup_fields
            .columns
            .iter()
            .map(|c| c.column_type)
            .collect();
        Ok(Box::new(LookupDataSet {
            join_kind: self.join_kind,
            input: dataset,
            lookup_source: self.lookup_source.clone(),
            key: self.key.clone(),
            output_schema: self.output_schema.clone(),
            lookup_field_names,
            lookup_field_types,
            buffer: VecDeque::with_capacity(self.lookup_source.batch_size()),
        }))
    }

    fn dump(&self) -> String {
        format!(
            "lookup {} from {} on {}",
            self.lookup_fields
                .columns
                .iter()
                .zip(
                    self.output_schema
                        .columns
                        .iter()
                        .skip(self.output_schema.columns.len() - self.lookup_fields.columns.len())
                )
                .map(|(field, new_field)| if field.name == new_field.name {
                    format!("{} as {}", field.name, field.column_type)
                } else {
                    format!(
                        "{} = {} as {}",
                        new_field.name, field.name, field.column_type
                    )
                })
                .collect::<Vec<String>>()
                .join(", "),
            self.lookup_source_name,
            self.key.dump()
        )
    }
}

struct LookupDataSet {
    join_kind: JoinKind,
    input: Box<dyn DataSet>,
    lookup_source: Arc<dyn LookupSource>,
    key: Arc<dyn Expression>,
    output_schema: Arc<Schema>,
    lookup_field_names: Vec<String>,
    lookup_field_types: Vec<ValueType>,

    buffer: VecDeque<Vec<Value>>,
}

#[async_trait]
impl DataSet for LookupDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        // Return anything left in the buffer
        if let Some(row) = self.buffer.pop_front() {
            return Some(row);
        }

        // Now nothing is in the buffer, so we need to fetch the next batch
        let mut buffered_input = Vec::new();
        while buffered_input.len() < self.lookup_source.batch_size() {
            if let Some(row) = self.input.next().await {
                buffered_input.push(row);
            } else {
                // The input is exhausted
                break;
            }
        }
        // End the stream if there are no more rows
        if buffered_input.is_empty() {
            return None;
        }

        // Run lookup in batch
        self.buffer = join_all(buffered_input.into_iter().map(|row| self.lookup(row)))
            .await
            .into_iter()
            .flatten()
            .collect();

        // Return the first row in the buffer
        self.buffer.pop_front()
    }
}

impl LookupDataSet {
    async fn lookup(&self, mut input_row: Vec<Value>) -> Vec<Vec<Value>> {
        let v = self.key.eval(&input_row);
        if v.is_error() {
            // Return all error row if key is error
            input_row.extend(vec![v; self.lookup_field_names.len()]);
            return vec![input_row];
        }
        match self.join_kind {
            JoinKind::Single => {
                let fields = self
                    .lookup_source
                    .lookup(&v, &self.lookup_field_names)
                    .await;
                let additional_fields = self
                    .lookup_field_types
                    .iter()
                    .zip(fields.into_iter())
                    .map(|(t, v)| v.cast_to(*t));
                input_row.extend(additional_fields);
                vec![input_row]
            }
            JoinKind::LeftInner => {
                // Return empty vec if the lookup is empty
                let lookup_rows = self.lookup_source.join(&v, &self.lookup_field_names).await;
                lookup_rows
                    .into_iter()
                    .map(|lookup_row| {
                        let additional_fields = self
                            .lookup_field_types
                            .iter()
                            .zip(lookup_row.into_iter())
                            .map(|(t, v)| v.cast_to(*t));
                        let mut ret = input_row.clone();
                        ret.extend(additional_fields);
                        ret
                    })
                    .collect()
            }
            JoinKind::LeftOuter => {
                // Return one row with Null lookup values
                let lookup_rows = self.lookup_source.join(&v, &self.lookup_field_names).await;
                let lookup_rows = if lookup_rows.is_empty() {
                    vec![vec![Value::Null; self.lookup_field_names.len()]]
                } else {
                    lookup_rows
                };
                lookup_rows
                    .into_iter()
                    .map(|lookup_row| {
                        let additional_fields = self
                            .lookup_field_types
                            .iter()
                            .zip(lookup_row.into_iter())
                            .map(|(t, v)| v.cast_to(*t));
                        let mut ret = input_row.clone();
                        ret.extend(additional_fields);
                        ret
                    })
                    .collect()
            }
        }
    }
}
