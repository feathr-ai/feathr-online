use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;

use crate::pipeline::{DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Clone, Debug)]
pub struct ProjectRemoveTransformation {
    output_schema: Arc<Schema>,
    removed_columns: Vec<String>,
    remove_set: HashSet<usize>,
}

impl ProjectRemoveTransformation {
    pub fn create(
        input_schema: &Schema,
        columns: Vec<String>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let output_schema = Arc::new(input_schema
            .columns
            .iter()
            .filter(|c| !columns.contains(&c.name))
            .cloned()
            .collect());
        let remove_set = input_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| columns.contains(&c.name))
            .map(|(i, _)| i)
            .collect();
        Ok(Box::new(Self {
            output_schema,
            removed_columns: columns,
            remove_set,
        }))
    }
}

impl Transformation for ProjectRemoveTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ProjectRemovedDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
            remove_set: self.remove_set.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!("project-remove {}", self.removed_columns.join(", "))
    }
}

struct ProjectRemovedDataSet {
    output_schema: Arc<Schema>,
    input: Box<dyn DataSet>,
    remove_set: HashSet<usize>,
}

#[async_trait]
impl DataSet for ProjectRemovedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        self.input.next().await.map(|row| {
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| !self.remove_set.contains(i))
                .map(|(_, v)| v)
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{pipeline::{pipelines::BuildContext, DataSetCreator, Value, Pipeline}, PiperError};

    #[tokio::test]
    async fn test_explode() {
        let pipeline = Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | project-remove b
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
        assert_eq!(schema, pipeline.output_schema);
        println!("pipelines: {}", pipeline.dump());
        println!("{:?}", rows);
        assert_eq!(rows.len(), 7);
        assert_eq!(rows[0], vec![Value::from(10)]);
        assert_eq!(rows[1], vec![Value::from(10)]);
        assert_eq!(rows[2], vec![Value::from(20)]);
        assert_eq!(rows[3], vec![Value::from(20)]);
        assert_eq!(rows[4], vec![Value::from(30)]);
        assert_eq!(rows[5], vec![Value::from(30)]);
        assert_eq!(rows[6], vec![Value::from(40)]);
    }
}