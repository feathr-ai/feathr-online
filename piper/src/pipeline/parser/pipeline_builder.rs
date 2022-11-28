use crate::pipeline::{Schema, pipelines::{Pipeline, Stage}, PiperError};

use super::transformation_builder::TransformationBuilder;

pub struct PipelineBuilder {
    pub name: String,
    pub input_schema: Schema,
    pub transformations: Vec<Box<dyn TransformationBuilder>>,
}

impl PipelineBuilder {
    pub fn build(&self) -> Result<Pipeline, PiperError> {
        let mut ret: Pipeline = Pipeline {
            name: self.name.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.input_schema.clone(),
            transformations: vec![],
        };
        for transformation_builder in &self.transformations {
            let transform = transformation_builder.build(&ret.output_schema)?;
            ret.output_schema = transform.get_output_schema(&ret.output_schema);
            ret.transformations.push(Stage::new(ret.output_schema.clone(), transform));
        }
        Ok(ret)
    }
}