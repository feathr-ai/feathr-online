use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use futures::future::join_all;

use crate::{
    pipeline::{
        expression::Expression, lookup::LookupSource, Column, DataSet, PiperError, Schema, Value,
        ValueType,
    },
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
        let fields = self
            .lookup_fields
            .columns
            .iter()
            .zip(
                self.output_schema
                    .columns
                    .iter()
                    .skip(self.output_schema.columns.len() - self.lookup_fields.columns.len()),
            )
            .map(|(field, new_field)| {
                if field.name == new_field.name {
                    format!("{} as {}", field.name, field.column_type)
                } else {
                    format!(
                        "{} = {} as {}",
                        new_field.name, field.name, field.column_type
                    )
                }
            })
            .collect::<Vec<String>>()
            .join(", ");
        if self.join_kind == JoinKind::Single {
            format!(
                "lookup {} from {} on {}",
                fields,
                self.lookup_source_name,
                self.key.dump()
            )
        } else {
            format!(
                "join kind={} {} from {} on {}",
                match self.join_kind {
                    JoinKind::Single => unreachable!(),
                    JoinKind::LeftInner => "left-inner",
                    JoinKind::LeftOuter => "left-outer",
                },
                fields,
                self.lookup_source_name,
                self.key.dump()
            )
        }
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

        // NOTE: `lookup()` may return empty results in the left-inner mode, so we need to loop until we get something
        while self.buffer.is_empty() {
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
        }

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
                // In single mode, lookup source returns exactly one row, even the key is not found
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
                vec![self.schema().convert(input_row)]
            }
            JoinKind::LeftInner => {
                // In LeftInner mode, return empty vec if the lookup result is empty
                // So the input row is gone if lookup result is empty
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
                        self.schema().convert(ret)
                    })
                    .collect()
            }
            JoinKind::LeftOuter => {
                // In LeftOuter mode, return one row with Null lookup values if the lookup result is empty
                // This behavior keeps the input row stay in the output dataset even if the lookup result is empty
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
                        self.schema().convert(ret)
                    })
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;

    use crate::{
        pipeline::{value::IntoValue, Column, DataSetCreator, Schema},
        LookupSource, Value, ValueType,
    };

    use super::{JoinKind, LookupTransformation};

    #[derive(Debug)]
    struct MockLookupSource {
        lookup_src: HashMap<Value, Vec<HashMap<String, Value>>>,
    }
    #[async_trait]
    impl LookupSource for MockLookupSource {
        async fn lookup(&self, key: &Value, fields: &[String]) -> Vec<Value> {
            self.join(key, fields)
                .await
                .get(0)
                .cloned()
                .unwrap_or_else(|| vec![Value::Null; fields.len()])
        }

        /**
         * It can return multiple rows in a join operation, if the lookup source supports it.
         */
        async fn join(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
            self.lookup_src
                .get(key)
                .map(|rs| {
                    rs.iter()
                        .map(|r| fields.iter().map(|f| r.get(f).unwrap().clone()).collect())
                        .collect()
                })
                .unwrap_or_default()
        }

        fn dump(&self) -> serde_json::Value {
            "mock".into()
        }
    }

    fn mock_lookup() -> Arc<dyn LookupSource> {
        let lookup = MockLookupSource {
            lookup_src: vec![
                (
                    1.into_value(),
                    vec![
                        vec![
                            ("name".to_string(), "a".into_value()),
                            ("age".to_string(), 20.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                        vec![
                            ("name".to_string(), "b".into_value()),
                            ("age".to_string(), 21.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                        vec![
                            ("name".to_string(), "c".into_value()),
                            ("age".to_string(), 22.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                    ],
                ),
                (
                    2.into_value(),
                    vec![
                        vec![
                            ("name".to_string(), "d".into_value()),
                            ("age".to_string(), 23.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                        vec![
                            ("name".to_string(), "e".into_value()),
                            ("age".to_string(), 24.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                    ],
                ),
                (
                    4.into_value(),
                    vec![
                        vec![
                            ("name".to_string(), "f".into_value()),
                            ("age".to_string(), 25.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                        vec![
                            ("name".to_string(), "g".into_value()),
                            ("age".to_string(), 26.into_value()),
                        ]
                        .into_iter()
                        .collect(),
                    ],
                ),
            ]
            .into_iter()
            .collect(),
        };
        Arc::new(lookup)
    }

    #[tokio::test]
    async fn test_lookup_inner() {
        let schema = Schema::from(vec![Column::new("key", ValueType::Int)]);
        let rows = vec![
            vec![1u32.into()],
            vec![2u32.into()],
            vec![3u32.into()],
            vec![4u32.into()],
        ];

        let input = DataSetCreator::eager(schema.clone(), rows.clone());
        let trans = LookupTransformation::create(
            JoinKind::LeftInner,
            &schema,
            "mock".to_string(),
            mock_lookup(),
            vec![
                ("name".to_string(), None, ValueType::String),
                ("age".to_string(), None, ValueType::Int),
            ],
            schema.get_col_expr("key").unwrap(),
        )
        .unwrap();

        let mut output = trans.transform(input).unwrap();
        let (output_schema, output_rows) = output.eval().await;
        println!("{:?}", output_schema);
        println!("{:?}", output_rows);
        assert_eq!(output_rows.len(), 7);
        assert_eq!(
            output_rows.iter().map(|r| r[1].clone()).collect::<Vec<_>>(),
            vec![
                "a".into_value(),
                "b".into_value(),
                "c".into_value(),
                "d".into_value(),
                "e".into_value(),
                "f".into_value(),
                "g".into_value()
            ]
        );
    }

    #[tokio::test]
    async fn test_lookup_outer() {
        let schema = Schema::from(vec![Column::new("key", ValueType::Int)]);
        let rows = vec![
            vec![1u32.into()],
            vec![2u32.into()],
            vec![3u32.into()],
            vec![4u32.into()],
        ];

        let input = DataSetCreator::eager(schema.clone(), rows.clone());
        let trans = LookupTransformation::create(
            JoinKind::LeftOuter,
            &schema,
            "mock".to_string(),
            mock_lookup(),
            vec![
                ("name".to_string(), None, ValueType::String),
                ("age".to_string(), None, ValueType::Int),
            ],
            schema.get_col_expr("key").unwrap(),
        )
        .unwrap();

        let mut output = trans.transform(input).unwrap();
        let (output_schema, output_rows) = output.eval().await;
        println!("transform: {}", trans.dump());
        println!("{:?}", output_schema);
        println!("{:?}", output_rows);
        assert_eq!(output_rows.len(), 8);
        assert_eq!(
            output_rows.iter().map(|r| r[1].clone()).collect::<Vec<_>>(),
            vec![
                "a".into_value(),
                "b".into_value(),
                "c".into_value(),
                "d".into_value(),
                "e".into_value(),
                Value::Null,
                "f".into_value(),
                "g".into_value()
            ]
        );
    }

    #[tokio::test]
    async fn test_lookup_single() {
        let schema = Schema::from(vec![Column::new("key", ValueType::Int)]);
        let rows = vec![
            vec![1u32.into()],
            vec![2u32.into()],
            vec![3u32.into()],
            vec![4u32.into()],
        ];

        let input = DataSetCreator::eager(schema.clone(), rows.clone());
        let trans = LookupTransformation::create(
            JoinKind::Single,
            &schema,
            "mock".to_string(),
            mock_lookup(),
            vec![
                ("name".to_string(), None, ValueType::String),
                ("age".to_string(), None, ValueType::Int),
            ],
            schema.get_col_expr("key").unwrap(),
        )
        .unwrap();

        let mut output = trans.transform(input).unwrap();
        let (output_schema, output_rows) = output.eval().await;
        println!("{:?}", output_schema);
        println!("{:?}", output_rows);
        assert_eq!(output_rows.len(), 4);
        assert_eq!(
            output_rows.iter().map(|r| r[1].clone()).collect::<Vec<_>>(),
            vec![
                "a".into_value(),
                "d".into_value(),
                Value::Null,
                "f".into_value(),
            ]
        );
    }
}
