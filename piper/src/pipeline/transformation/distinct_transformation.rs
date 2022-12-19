use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;

use crate::{
    pipeline::{expression::Expression, DataSet, Schema},
    PiperError, Value,
};

use super::Transformation;

#[derive(Debug)]
pub struct DistinctTransformation {
    pub key_fields: Arc<Vec<Box<dyn Expression>>>,
    pub output_schema: Schema,
}

impl Transformation for DistinctTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(DistinctDataSet {
            input: dataset,
            key_fields: self.key_fields.clone(),
            output_schema: self.output_schema.clone(),
            seen_keys: HashSet::new(),
        }))
    }

    fn dump(&self) -> String {
        let keys = self
            .output_schema
            .columns
            .iter()
            .zip(self.key_fields.iter())
            .map(|(col, expr)| format!("{}={}", col.name, expr.dump()))
            .collect::<Vec<_>>()
            .join(", ");
        format!("distinct by {}", keys)
    }
}

struct DistinctDataSet {
    input: Box<dyn DataSet>,
    key_fields: Arc<Vec<Box<dyn Expression>>>,
    output_schema: Schema,
    seen_keys: HashSet<Vec<Value>>,
}

#[async_trait]
impl DataSet for DistinctDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        while let Some(row) = self.input.next().await {
            let key = self
                .key_fields
                .iter()
                .map(|e| e.eval(&row))
                .collect::<Vec<_>>();
            if self.seen_keys.insert(key.clone()) {
                return Some(key);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{pipelines::BuildContext, DataSetCreator, Value, Pipeline};

    #[tokio::test]
    async fn test_distinct() {
        let pipeline = Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | distinct by a
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
                vec![Value::from(30), Value::from(vec![4, 5, 6])],
                vec![Value::from(30), Value::from(vec![600])],
                vec![Value::from(40), Value::from(Vec::<i32>::new())],
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
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], 10.into());
        assert_eq!(rows[1][0], 20.into());
        assert_eq!(rows[2][0], 30.into());
        assert_eq!(rows[3][0], 40.into());
    }
}
