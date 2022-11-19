use std::collections::VecDeque;

use async_trait::async_trait;
use serde::{Serialize, Deserialize};

use super::{PiperError, Value, ValueType};

/**
 * The column definition
 */
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    /**
     * Column name
     */
    pub name: String,

    /**
     * Column type
     */
    pub column_type: ValueType,
}

impl Column {
    pub fn new<T>(name: T, column_type: ValueType) -> Self
    where
        T: ToString,
    {
        Self {
            name: name.to_string(),
            column_type,
        }
    }
}

/**
 * Schema is a collection of columns
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl<T> From<T> for Schema
where
    T: IntoIterator<Item = Column>,
{
    fn from(columns: T) -> Self {
        Self {
            columns: columns.into_iter().collect(),
        }
    }
}

impl FromIterator<Column> for Schema {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        Self {
            columns: iter.into_iter().collect(),
        }
    }
}

impl Schema {
    pub fn new() -> Self {
        Schema { columns: vec![] }
    }

    pub fn get_column_types(&self) -> Vec<ValueType> {
        self.columns.iter().map(|c| c.column_type).collect()
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.name == column_name)
    }

    pub fn dump(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!("{} as {}", c.name, c.column_type))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/**
 * Define how the data set is validated
 */
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    /**
     * The row will become a failure if there is any field doesn't match the schema, already failed rows remain failed.
     */
    Strict,

    /**
     * The row will be skipped if there is any field doesn't match the schema.
     */
    Skip,

    /**
     * The row that has column doesn't match the schema will be skipped, and failed rows will be skipped.
     */
    Lenient,

    /**
     * The unmatched column will be converted to the schema type, and turned into `null` if failed, failed rows will be skipped
     */
    Convert,
}

impl Default for ValidationMode {
    fn default() -> Self {
        ValidationMode::Convert
    }
}

/**
 * The DataSet interface
 * A DataSet is a collection of rows, each row is a collection of fields.
 * DataSet works like an iterator, it can only be used once.
 */
#[async_trait]
pub trait DataSet: Sync + Send {
    /**
     * Get the schema of the data set
     */
    fn schema(&self) -> &Schema;

    /**
     * Get the next row of the data set, returns None if there is no more row
     */
    async fn next(&mut self) -> Option<Result<Vec<Value>, PiperError>>;

    /**
     * Get all rows of the data set
     */
    async fn eval(&mut self) -> (Schema, Vec<Result<Vec<Value>, PiperError>>) {
        let mut rows = Vec::new();
        while let Some(row) = self.next().await {
            rows.push(row);
        }
        (self.schema().clone(), rows)
    }

    async fn dump(&mut self) -> String {
        let mut ret = String::new();
        let s = self.schema().dump();
        ret.push_str(&s);
        ret.push('\n');
        ret.push_str("-".repeat(s.len()).as_str());
        ret.push('\n');
        for row in self.next().await {
            match row {
                Ok(row) => {
                    ret.push_str(
                        row.iter()
                            .map(|v| v.dump())
                            .collect::<Vec<_>>()
                            .join(", ")
                            .as_str(),
                    );
                }
                Err(e) => {
                    ret.push_str(&format!("Error: {}\n", e));
                }
            }
            ret.push('\n');
        }
        ret
    }
}

/**
 * Some common operations to create a data set
 */
pub struct DataSetCreator;

#[allow(dead_code)]
impl DataSetCreator {
    /**
     * Create an empty data set which contains no row
     */
    pub fn empty(schema: Schema) -> Box<dyn DataSet> {
        EagerDataSet::new(schema, vec![])
    }

    /**
     * Create a data set from a vector of rows
     */
    pub fn eager<T>(schema: Schema, rows: T) -> Box<dyn DataSet>
    where
        T: IntoIterator<Item = Vec<Value>>,
    {
        EagerDataSet::new(schema, rows.into_iter().collect())
    }

    /**
     * Create a data set from an iterator
     */
    pub fn from<T>(rows: T) -> Box<dyn DataSet>
    where
        T: IntoIterator<Item = Vec<Value>>,
    {
        rows.into_iter().collect::<Box<EagerDataSet>>()
    }
}

pub trait DataSetValidator {
    /**
     * Validate the data set, make sure all rows match the schema.
     * `validate_mode` defines how the rows are validated
     */
    fn validated(self, validate_mode: ValidationMode) -> Box<dyn DataSet>;
}

impl DataSetValidator for Box<dyn DataSet> {
    fn validated(self, validate_mode: ValidationMode) -> Box<dyn DataSet> {
        Box::new(ValidatedDataSet {
            inner: self,
            validate_mode,
        })
    }
}

struct ValidatedDataSet {
    inner: Box<dyn DataSet>,
    validate_mode: ValidationMode,
}

impl ValidatedDataSet {
    fn validate_row(
        &self,
        row: Result<Vec<Value>, PiperError>,
    ) -> Option<Result<Vec<Value>, PiperError>> {
        match self.validate_mode {
            ValidationMode::Strict => {
                match row {
                    Ok(row) => {
                        // Fail if row length doesn't match
                        if row.len() != self.inner.schema().columns.len() {
                            return Some(Err(PiperError::InvalidRowLength(
                                self.inner.schema().columns.len(),
                                row.len(),
                            )));
                        }
                        // Fail if any field type doesn't match
                        Some(
                            row.into_iter()
                                .zip(self.inner.schema().columns.iter())
                                .map(|(field, column)| field.try_into(column.column_type))
                                .collect::<Result<Vec<_>, _>>(),
                        )
                    }
                    // Retain upstream error
                    Err(e) => Some(Err(e)),
                }
            }
            ValidationMode::Skip => {
                match row {
                    Ok(row) => {
                        // Fail if row length doesn't match
                        if row.len() != self.inner.schema().columns.len() {
                            return Some(Err(PiperError::InvalidRowLength(
                                self.inner.schema().columns.len(),
                                row.len(),
                            )));
                        }
                        // Fail if any field type doesn't match
                        let ret = Some(
                            row.into_iter()
                                .zip(self.inner.schema().columns.iter())
                                .map(|(field, column)| field.try_into(column.column_type))
                                .collect::<Result<Vec<_>, _>>(),
                        );
                        // Skip error
                        if let Some(Err(_)) = ret {
                            return None;
                        }
                        ret
                    }
                    // Skip upstream error
                    Err(_) => None,
                }
            }
            ValidationMode::Lenient => {
                match row {
                    // Field is set to null if type doesn't match
                    Ok(mut row) => {
                        row.resize(self.inner.schema().columns.len(), Value::Null);
                        let ret = row
                            .into_iter()
                            .zip(self.inner.schema().columns.iter())
                            .map(|(field, column)| {
                                field.try_into(column.column_type).unwrap_or(Value::Null)
                            })
                            .collect::<Vec<_>>();
                        Some(Ok(ret))
                    }
                    // Skip upstream error
                    Err(_) => None,
                }
            }
            ValidationMode::Convert => {
                match row {
                    // Field is set to null if it cannot be converted
                    Ok(mut row) => {
                        row.resize(self.inner.schema().columns.len(), Value::Null);
                        let ret = row
                            .into_iter()
                            .zip(self.inner.schema().columns.iter())
                            .map(|(field, column)| {
                                field.try_convert(column.column_type).unwrap_or(Value::Null)
                            })
                            .collect::<Vec<_>>();
                        Some(Ok(ret))
                    }
                    // Skip upstream error
                    Err(_) => None,
                }
            }
        }
    }
}

#[async_trait]
impl DataSet for ValidatedDataSet {
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    async fn next(&mut self) -> Option<Result<Vec<Value>, PiperError>> {
        let mut ret = None;
        while ret.is_none() {
            let row = self.inner.next().await;
            match row {
                Some(row) => {
                    ret = self.validate_row(row);
                }
                None => {
                    // Upstream returns None, means it's exhausted.
                    return None;
                }
            }
        }
        ret
    }
}

#[derive(Clone, Debug)]
struct EagerDataSet {
    schema: Schema,
    rows: VecDeque<Vec<Value>>,
}

impl EagerDataSet {
    fn new(schema: Schema, rows: Vec<Vec<Value>>) -> Box<Self> {
        Box::new(Self {
            schema,
            rows: rows.into(),
        })
    }
}

impl FromIterator<Vec<Value>> for Box<EagerDataSet> {
    fn from_iter<T: IntoIterator<Item = Vec<Value>>>(iter: T) -> Self {
        let mut rows = vec![];
        let mut schema = None;
        let mut col = 0;
        for row in iter {
            if schema.is_none() {
                schema = Some(Schema::from_iter(row.iter().map(|v| {
                    col += 1;
                    Column::new(format!("col{}", col), v.value_type())
                })));
            }
            rows.push(row);
        }
        Box::new(EagerDataSet {
            schema: schema.unwrap(),
            rows: rows.into(),
        })
    }
}

impl From<Vec<Vec<Value>>> for Box<EagerDataSet> {
    fn from(rows: Vec<Vec<Value>>) -> Self {
        let schema: Schema = rows[0]
            .iter()
            .enumerate()
            .map(|(i, v)| Column::new(i, v.value_type()))
            .collect();
        EagerDataSet::new(schema, rows)
    }
}

#[async_trait]
impl DataSet for EagerDataSet {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn next(&mut self) -> Option<Result<Vec<Value>, PiperError>> {
        self.rows.pop_front().map(|row| Ok(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen_schema() -> Schema {
        vec![
            Column::new("col1", ValueType::Int),
            Column::new("col2", ValueType::String),
            Column::new("col2", ValueType::Bool),
        ]
        .into()
    }

    fn gen_ds() -> Box<dyn DataSet> {
        DataSetCreator::eager(
            gen_schema(),
            vec![
                vec![Value::from(10), Value::from(100), Value::from(true)],
                vec![Value::from(20), Value::from("foo"), Value::from(true)],
                vec![Value::from(30), Value::from(300), Value::from(false)],
                vec![Value::from(40), Value::from(400), Value::from(false)],
                vec![Value::from(50), Value::from(500), Value::from(false)],
                vec![Value::from(60), Value::from("bar"), Value::from(false)],
                vec![Value::from(70), Value::from(700), Value::from(true)],
            ],
        )
    }

    #[tokio::test]
    async fn test_validate_strict() {
        let (schema, rows) = gen_ds().validated(ValidationMode::Strict).eval().await;
        assert_eq!(gen_schema(), schema);
        assert!(rows[0].is_err());
        assert!(rows[1].is_ok());
        assert!(rows[2].is_err());
        assert!(rows[3].is_err());
        assert!(rows[4].is_err());
        assert!(rows[5].is_ok());
        assert!(rows[6].is_err());
    }

    #[tokio::test]
    async fn test_validate_skip() {
        let (schema, rows) = gen_ds().validated(ValidationMode::Skip).eval().await;
        assert_eq!(gen_schema(), schema);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].is_ok());
        assert!(rows[1].is_ok());
    }
}
