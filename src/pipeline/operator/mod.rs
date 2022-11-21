use std::fmt::Debug;

use dyn_clonable::clonable;

use super::{PiperError, Value, ValueType};

mod math_op;
mod comparison_op;
mod logical_op;
mod unary_op;
mod index_op;
mod function_op;

pub use math_op::{PlusOperator, MinusOperator, MultiplyOperator, DivideOperator};
pub use comparison_op::{GreaterThanOperator, LessThanOperator, GreaterEqualOperator, LessEqualOperator, EqualOperator, NotEqualOperator};
pub use logical_op::{AndOperator, OrOperator};
pub use unary_op::{NotOperator, NegativeOperator, PositiveOperator, IsNullOperator, IsNotNullOperator};
pub use index_op::{ArrayIndexOperator, MapIndexOperator};
pub use function_op::FunctionOperator;

#[clonable]
pub trait Operator: Clone + Debug + Send + Sync {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError>;

    fn eval(&self, arguments: Vec<Value>) -> Value;

    fn dump(&self, arguments: Vec<String>) -> String;
}
