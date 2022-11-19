use crate::pipeline::{transformation::Transformation, Schema, PiperError};

mod take_builder;
mod where_builder;
mod project_builder;
mod project_rename_builder;
mod project_remove_builder;
mod explode_builder;
mod lookup_builder;
mod top_builder;
mod ignore_error_builder;

pub use take_builder::TakeTransformationBuilder;
pub use where_builder::WhereTransformationBuilder;
pub use project_builder::ProjectTransformationBuilder;
pub use project_rename_builder::ProjectRenameTransformationBuilder;
pub use project_remove_builder::ProjectRemoveTransformationBuilder;
pub use explode_builder::ExplodeTransformationBuilder;
pub use lookup_builder::LookupTransformationBuilder;
pub use top_builder::TopTransformationBuilder;
pub use ignore_error_builder::IgnoreErrorTransformationBuilder;

pub trait TransformationBuilder {
    fn build(&self, input_schema: &Schema) -> Result<Box<dyn Transformation>, PiperError>;
}
