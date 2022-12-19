use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use crate::pipeline::{Column, DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Debug, Clone)]
pub struct ProjectRenameTransformation {
    output_schema: Arc<Schema>,
    renames: HashMap<String, String>,
}

impl ProjectRenameTransformation {
    pub fn create(
        input_schema: &Schema,
        renames: HashMap<String, String>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(Self {
            output_schema: Arc::new(
                input_schema
                    .columns
                    .iter()
                    .map(|c| Column {
                        name: renames.get(&c.name).unwrap_or(&c.name).to_owned(),
                        column_type: c.column_type,
                    })
                    .collect(),
            ),
            renames,
        }))
    }
}

impl Transformation for ProjectRenameTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ProjectRenamedDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!(
            "project-rename {}",
            self.renames
                .iter()
                .map(|(old, new)| format!("{} = {}", old, new))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

struct ProjectRenamedDataSet {
    output_schema: Arc<Schema>,
    input: Box<dyn DataSet>,
}

#[async_trait]
impl DataSet for ProjectRenamedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }
    async fn next(&mut self) -> Option<Vec<Value>> {
        self.input.next().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::{pipelines::BuildContext, DataSetCreator, Pipeline, Value},
        PiperError,
    };

    #[tokio::test]
    async fn test_explode() {
        let pipeline = Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | project-rename c = a, d = b
            ;",
            &BuildContext::default(),
        )
        .unwrap();
        let src_rows = vec![
            vec![Value::from(10), Value::from(vec![1, 2, 3])],
            vec![Value::from(10), Value::from(Vec::<i32>::new())],
            vec![Value::from(20), Value::from(Vec::<i32>::new())],
            vec![Value::from(20), Value::from(vec![400])],
            vec![
                Value::from(30),
                Value::Error(PiperError::Unknown("test".to_string())),
            ],
            vec![Value::from(30), Value::from(vec![600])],
            vec![Value::from(40), Value::from(vec![800])],
        ];
        let ds = DataSetCreator::eager(pipeline.input_schema.clone(), src_rows.clone());
        let (schema, rows) = pipeline
            .process(ds, crate::pipeline::ValidationMode::Strict)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema.columns.len(), pipeline.output_schema.columns.len());
        assert_eq!(
            schema.columns[0].column_type,
            pipeline.output_schema.columns[0].column_type
        );
        assert_eq!(
            schema.columns[1].column_type,
            pipeline.output_schema.columns[1].column_type
        );
        assert_eq!(pipeline.output_schema.columns[0].name, "c");
        assert_eq!(pipeline.output_schema.columns[1].name, "d");
        println!("pipelines: {}", pipeline.dump());
        println!("{:?}", rows);
        assert_eq!(rows, src_rows);
    }
}
