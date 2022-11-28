use std::fmt::Debug;

use super::{PiperError, Value, ValueType};

mod comparison_op;
mod function_op;
mod index_op;
mod logical_op;
mod math_op;
mod unary_op;

pub use comparison_op::{
    EqualOperator, GreaterEqualOperator, GreaterThanOperator, LessEqualOperator, LessThanOperator,
    NotEqualOperator,
};
pub use function_op::FunctionOperator;
pub use index_op::{ArrayIndexOperator, MapIndexOperator};
pub use logical_op::{AndOperator, OrOperator};
pub use math_op::{
    DivOperator, DivideOperator, MinusOperator, ModOperator, MultiplyOperator, PlusOperator,
};
pub use unary_op::{
    IsNotNullOperator, IsNullOperator, NegativeOperator, NotOperator, PositiveOperator,
};

pub trait Operator: Debug + Send + Sync {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError>;

    fn eval(&self, arguments: Vec<Value>) -> Value;

    fn dump(&self, arguments: Vec<String>) -> String;
}
