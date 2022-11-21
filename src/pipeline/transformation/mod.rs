use std::fmt::Debug;

use super::{DataSet, PiperError, Schema};

mod take_transformation;
mod where_transformation;
mod project_transformation;
mod project_rename_transformation;
mod project_remove_transformation;
mod project_keep_transformation;
mod explode_transformation;
mod lookup_transformation;
mod top_transformation;
mod ignore_error_transformation;

pub use take_transformation::TakeTransformation;
pub use where_transformation::WhereTransformation;
pub use project_transformation::ProjectTransformation;
pub use project_rename_transformation::ProjectRenameTransformation;
pub use project_remove_transformation::ProjectRemoveTransformation;
pub use project_keep_transformation::ProjectKeepTransformation;
pub use explode_transformation::ExplodeTransformation;
pub use lookup_transformation::LookupTransformation;
pub use top_transformation::{TopTransformation, SortOrder, NullPos};
pub use ignore_error_transformation::IgnoreErrorTransformation;

pub trait Transformation: Sync + Send + Debug {
    fn get_output_schema(&self, input_schema: &Schema) -> Schema;
    fn transform(
        &self,
        dataset: Box<dyn DataSet>,
    ) -> Result<Box<dyn DataSet>, PiperError>;

    fn dump(&self) -> String;
}

