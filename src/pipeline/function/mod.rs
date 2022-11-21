use std::collections::HashMap;
use std::fmt::Debug;

use once_cell::sync::OnceCell;

use super::{PiperError, Value, ValueType};

mod bucket;
mod case;
mod extract_json;
mod len;
mod make_array;
mod math;
mod split;
mod substring;
mod timestamp;
mod to_json;
mod type_conv;

use bucket::BucketFunction;
use case::CaseFunction;
use extract_json::{ExtractJsonArray, ExtractJsonObject};
use len::Len;
use make_array::MakeArray;
use math::*;
use split::SplitFunction;
use substring::SubstringFunction;
use timestamp::TimestampFunction;
use to_json::ToJsonStringFunction;
use type_conv::TypeConverterFunction;

pub trait Function: Send + Sync + Debug {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError>;

    fn eval(&self, arguments: Vec<Value>) -> Value;
}

static FUNCTION_REPO: OnceCell<HashMap<String, Box<dyn Function + 'static>>> = OnceCell::new();

pub fn get_function(name: &str) -> Option<(&'static str, &'static dyn Function)> {
    FUNCTION_REPO
        .get_or_init(init_built_in_functions)
        .get_key_value(name)
        .map(|(name, f)| (name.as_str(), f.as_ref()))
}

fn init_built_in_functions() -> HashMap<String, Box<dyn Function + 'static>> {
    // Built-in functions
    let mut function_map: HashMap<String, Box<dyn Function + 'static>> = HashMap::new();
    function_map.insert("abs".to_string(), Box::new(Abs));
    function_map.insert("ceil".to_string(), Box::new(Ceil));
    function_map.insert("floor".to_string(), Box::new(Floor));
    function_map.insert("round".to_string(), Box::new(Round));

    function_map.insert("sin".to_string(), Box::new(Sin));
    function_map.insert("cos".to_string(), Box::new(Cos));
    function_map.insert("tan".to_string(), Box::new(Tan));
    function_map.insert("sinh".to_string(), Box::new(Sinh));
    function_map.insert("cosh".to_string(), Box::new(Cosh));
    function_map.insert("tanh".to_string(), Box::new(Tanh));
    function_map.insert("asin".to_string(), Box::new(Asin));
    function_map.insert("acos".to_string(), Box::new(Acos));
    function_map.insert("atan".to_string(), Box::new(Atan));
    function_map.insert("asinh".to_string(), Box::new(Asinh));
    function_map.insert("acosh".to_string(), Box::new(Acosh));
    function_map.insert("atanh".to_string(), Box::new(Atanh));

    function_map.insert("sqrt".to_string(), Box::new(Sqrt));
    function_map.insert("cbrt".to_string(), Box::new(Cbrt));
    function_map.insert("exp".to_string(), Box::new(Exp));
    function_map.insert("ln".to_string(), Box::new(Ln));
    function_map.insert("log10".to_string(), Box::new(Log10));
    function_map.insert("log2".to_string(), Box::new(Log2));
    function_map.insert("log".to_string(), Box::new(Log));
    function_map.insert("pow".to_string(), Box::new(Pow));

    function_map.insert("substring".to_string(), Box::new(SubstringFunction));
    function_map.insert("split".to_string(), Box::new(SplitFunction));
    function_map.insert("case".to_string(), Box::new(CaseFunction));
    function_map.insert("bucket".to_string(), Box::new(BucketFunction));
    function_map.insert("timestamp".to_string(), Box::new(TimestampFunction));
    function_map.insert("make_array".to_string(), Box::new(MakeArray));
    function_map.insert("len".to_string(), Box::new(Len));
    function_map.insert("extract_json".to_string(), Box::new(ExtractJsonObject));
    function_map.insert("extract_json_array".to_string(), Box::new(ExtractJsonArray));
    function_map.insert("to_json".to_string(), Box::new(ToJsonStringFunction));
    function_map.insert(
        "to_bool".to_string(),
        Box::new(TypeConverterFunction {
            to: ValueType::Bool,
        }),
    );
    function_map.insert(
        "to_int".to_string(),
        Box::new(TypeConverterFunction { to: ValueType::Int }),
    );
    function_map.insert(
        "to_long".to_string(),
        Box::new(TypeConverterFunction {
            to: ValueType::Long,
        }),
    );
    function_map.insert(
        "to_float".to_string(),
        Box::new(TypeConverterFunction {
            to: ValueType::Float,
        }),
    );
    function_map.insert(
        "to_double".to_string(),
        Box::new(TypeConverterFunction {
            to: ValueType::Double,
        }),
    );
    function_map.insert(
        "to_string".to_string(),
        Box::new(TypeConverterFunction {
            to: ValueType::String,
        }),
    );
    function_map
}
