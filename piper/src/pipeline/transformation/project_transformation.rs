use std::sync::Arc;

use async_trait::async_trait;

use crate::pipeline::{expression::{Expression, ColumnExpression}, Column, DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Debug)]
pub struct ProjectTransformation {
    output_schema: Arc<Schema>,
    columns: Arc<Vec<Box<dyn Expression>>>,
    column_names: Vec<String>,
}

impl ProjectTransformation {
    pub fn create(
        input_schema: &Schema,
        columns: Vec<(String, Box<dyn Expression>)>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let column_names = columns.iter().map(|(c, _)| c.clone()).collect();
        let addition_columns = columns
            .iter()
            .map(|(name, exp)| {
                let column_type = exp.get_output_type(&input_schema.get_column_types())?;
                Ok(Column {
                    name: name.clone(),
                    column_type,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut col_expr = input_schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Box::new(ColumnExpression {
                    column_index: i,
                    column_name: c.name.clone(),
                }) as Box<dyn Expression>
            })
            .collect::<Vec<_>>();
        col_expr.extend(columns.into_iter().map(|(_, exp)| exp));
        let output_schema = Arc::new(input_schema
            .columns
            .clone()
            .into_iter()
            .chain(addition_columns)
            .collect());
        Ok(Box::new(Self {
            output_schema,
            columns: Arc::new(col_expr),
            column_names,
        }))
    }
}

impl Transformation for ProjectTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ProjectedDataSet {
            input_dataset: dataset,
            output_schema: self.output_schema.clone(),
            columns: self.columns.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!(
            "project {}",
            self.column_names
                .iter()
                .zip(
                    self.columns
                        .iter()
                        .skip(self.columns.len() - self.column_names.len())
                )
                .map(|(c, e)| format!("{} = {}", c, e.dump()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

struct ProjectedDataSet {
    input_dataset: Box<dyn DataSet>,
    output_schema: Arc<Schema>,
    columns: Arc<Vec<Box<dyn Expression>>>,
}

#[async_trait]
impl DataSet for ProjectedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        match self.input_dataset.next().await {
            Some(row) => {
                let mut output_row = Vec::with_capacity(self.columns.len());
                for col in self.columns.as_ref().iter() {
                    output_row.push(col.eval(&row));
                }
                Some(output_row)
            }
            None => None,
        }
    }
}
