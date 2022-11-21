use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

macro_rules! unary_impl_math_function {
    ($name:ident, $op:ident, $call:ident) => {
        #[derive(Debug)]
        pub struct $name;

        impl Function for $name {
            fn get_output_type(
                &self,
                argument_types: &[crate::pipeline::ValueType],
            ) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
                if argument_types.len() != 1 {
                    return Err(PiperError::ArityError(
                        stringify!($op).to_string(),
                        argument_types.len(),
                    ));
                }
                if !argument_types[0].is_numeric() {
                    return Err(PiperError::InvalidArgumentType(
                        stringify!($op).to_string(),
                        0,
                        argument_types[0],
                    ));
                }
                Ok(ValueType::Double)
            }

            fn eval(&self, arguments: Vec<crate::pipeline::Value>) -> Value {
                if arguments.len() != 1 {
                    return Value::Error(PiperError::InvalidArgumentCount(1, arguments.len()));
                }
                let v = match arguments[0].get_double() {
                    Ok(v) => v,
                    Err(e) => return Value::Error(e),
                };
                Value::Double(v.$call()).into()
            }
        }
    };
}

unary_impl_math_function!(Ceil, ceil, ceil);
unary_impl_math_function!(Floor, floor, floor);
unary_impl_math_function!(Round, round, round);

unary_impl_math_function!(Sin, sin, sin);
unary_impl_math_function!(Cos, cos, cos);
unary_impl_math_function!(Tan, tan, tan);
unary_impl_math_function!(Asin, asin, asin);
unary_impl_math_function!(Acos, acos, acos);
unary_impl_math_function!(Atan, atan, atan);
unary_impl_math_function!(Sinh, sinh, sinh);
unary_impl_math_function!(Cosh, cosh, cosh);
unary_impl_math_function!(Tanh, tanh, tanh);
unary_impl_math_function!(Asinh, asinh, asinh);
unary_impl_math_function!(Acosh, acosh, acosh);
unary_impl_math_function!(Atanh, atanh, atanh);

unary_impl_math_function!(Sqrt, sqrt, sqrt);
unary_impl_math_function!(Cbrt, cbrt, cbrt);
unary_impl_math_function!(Exp, exp, exp);
unary_impl_math_function!(Ln, ln, ln);
unary_impl_math_function!(Log10, log10, log10);
unary_impl_math_function!(Log2, log2, log2);

macro_rules! binary_impl_math_function {
    ($name:ident, $op:ident, $call:ident) => {
        #[derive(Debug)]
        pub struct $name;

        impl Function for $name {
            fn get_output_type(
                &self,
                argument_types: &[crate::pipeline::ValueType],
            ) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
                if argument_types.len() != 2 {
                    return Err(PiperError::ArityError(
                        stringify!($op).to_string(),
                        argument_types.len(),
                    ));
                }
                if !argument_types[0].is_numeric() {
                    return Err(PiperError::InvalidArgumentType(
                        stringify!($op).to_string(),
                        0,
                        argument_types[0],
                    ));
                }
                if !argument_types[1].is_numeric() {
                    return Err(PiperError::InvalidArgumentType(
                        stringify!($op).to_string(),
                        0,
                        argument_types[0],
                    ));
                }
                Ok(ValueType::Double)
            }

            fn eval(&self, arguments: Vec<crate::pipeline::Value>) -> Value {
                if arguments.len() != 2 {
                    return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
                }
                let l = match arguments[0].get_double() {
                    Ok(v) => v,
                    Err(e) => return Value::Error(e),
                };
                let r = match arguments[0].get_double() {
                    Ok(v) => v,
                    Err(e) => return Value::Error(e),
                };
                Value::Double(l.$call(r)).into()
            }
        }
    };
}

binary_impl_math_function!(Log, log, log);
binary_impl_math_function!(Pow, pow, powf);

#[derive(Debug)]
pub struct Abs;

impl Function for Abs {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                stringify!($op).to_string(),
                argument_types.len(),
            ));
        }
        if !argument_types[0].is_numeric() {
            return Err(PiperError::InvalidArgumentType(
                stringify!($op).to_string(),
                0,
                argument_types[0],
            ));
        }
        Ok(argument_types[0])
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        match arguments[0] {
            Value::Int(v) => Value::Int(v.abs()),
            Value::Long(v) => Value::Long(v.abs()),
            Value::Float(v) => Value::Float(v.abs()),
            Value::Double(v) => Value::Double(v.abs()),
            _ => unreachable!(),
        }
    }
}
