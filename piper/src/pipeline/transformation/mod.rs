use std::fmt::Debug;

use super::{DataSet, PiperError, Schema};

mod distinct_transformation;
mod explode_transformation;
mod ignore_error_transformation;
mod lookup_transformation;
mod project_keep_transformation;
mod project_remove_transformation;
mod project_rename_transformation;
mod project_transformation;
mod summarize_transformation;
mod take_transformation;
mod top_transformation;
mod where_transformation;

pub use distinct_transformation::DistinctTransformation;
pub use explode_transformation::ExplodeTransformation;
pub use ignore_error_transformation::IgnoreErrorTransformation;
pub use lookup_transformation::{JoinKind, LookupTransformation};
pub use project_keep_transformation::ProjectKeepTransformation;
pub use project_remove_transformation::ProjectRemoveTransformation;
pub use project_rename_transformation::ProjectRenameTransformation;
pub use project_transformation::ProjectTransformation;
pub use summarize_transformation::SummarizeTransformation;
pub use take_transformation::TakeTransformation;
pub use top_transformation::{NullPos, SortOrder, TopTransformation};
pub use where_transformation::WhereTransformation;

pub trait Transformation: Sync + Send + Debug {
    fn get_output_schema(&self, input_schema: &Schema) -> Schema;
    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError>;

    fn dump(&self) -> String;
}
