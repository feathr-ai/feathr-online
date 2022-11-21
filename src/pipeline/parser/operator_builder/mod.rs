use std::fmt::Debug;

use super::super::{
    operator::*,
    PiperError,
};

mod binary_op_builder;
mod unary_op_builder;
mod function_op_builder;

pub use binary_op_builder::BinaryOperatorBuilder;
pub use unary_op_builder::UnaryOperatorBuilder;
pub use function_op_builder::FunctionOperatorBuilder;

pub trait OperatorBuilder : Debug {
    fn build(&self) -> Result<Box<dyn Operator>, PiperError>;
}

