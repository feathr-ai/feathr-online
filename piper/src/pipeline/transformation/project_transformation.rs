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
#[cfg(test)]
mod tests {
    use crate::{pipeline::{pipelines::BuildContext, DataSetCreator, Value, Pipeline}, PiperError};

    #[tokio::test]
    async fn test_explode() {
        let pipeline = Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | project c = a+1
            ;",
            &BuildContext::default(),
        )
        .unwrap();
        let ds = DataSetCreator::eager(
            pipeline.input_schema.clone(),
            vec![
                vec![Value::from(10), Value::from(vec![1, 2, 3])],
                vec![Value::from(10), Value::from(Vec::<i32>::new())],
                vec![Value::from(20), Value::from(Vec::<i32>::new())],
                vec![Value::from(20), Value::from(vec![400])],
                vec![Value::from(30), Value::Error(PiperError::Unknown("test".to_string()))],
                vec![Value::from(30), Value::from(vec![600])],
                vec![Value::from(40), Value::from(vec![800])],
            ],
        );
        let (schema, rows) = pipeline
            .process(ds, crate::pipeline::ValidationMode::Strict)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema.columns[0], pipeline.output_schema.columns[0]);
        assert_eq!(schema.columns[1], pipeline.output_schema.columns[1]);
        assert_eq!(pipeline.output_schema.columns[2].name, "c");
        assert!(pipeline.output_schema.columns[2].column_type.is_numeric());
        println!("pipelines: {}", pipeline.dump());
        println!("{:?}", rows);
        assert_eq!(rows.len(), 7);
        assert_eq!(rows[0][2], Value::from(11));
        assert_eq!(rows[1][2], Value::from(11));
        assert_eq!(rows[2][2], Value::from(21));
        assert_eq!(rows[3][2], Value::from(21));
        assert_eq!(rows[4][2], Value::from(31));
        assert_eq!(rows[5][2], Value::from(31));
        assert_eq!(rows[6][2], Value::from(41));
    }
}