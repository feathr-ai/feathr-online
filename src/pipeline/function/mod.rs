use std::collections::HashMap;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use once_cell::sync::OnceCell;

use self::function_wrapper::var_fn;

use super::{PiperError, Value, ValueType};

mod array_functions;
mod bucket;
mod case;
mod datetime_functions;
mod extract_json;
mod function_wrapper;
mod len;
mod make_array;
mod misc_functions;
mod rand_functions;
mod split;
mod substring;
mod timestamp;
mod to_json;
mod type_conv;

use array_functions::*;
use bucket::BucketFunction;
use case::CaseFunction;
use datetime_functions::*;
use extract_json::{ExtractJsonArray, ExtractJsonObject};
use function_wrapper::{binary_fn, nullary_fn, ternary_fn, unary_fn};
use len::Len;
use make_array::MakeArray;
use misc_functions::*;
use split::SplitFunction;
use substring::SubstringFunction;
use timestamp::TimestampFunction;
use to_json::ToJsonStringFunction;
use type_conv::TypeConverterFunction;

pub trait Function: Send + Sync {
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

#[rustfmt::skip]
fn init_built_in_functions() -> HashMap<String, Box<dyn Function + 'static>> {
    // Built-in functions
    let mut function_map: HashMap<String, Box<dyn Function + 'static>> = HashMap::new();
    function_map.insert("abs".to_string(), Box::new(Abs));
    function_map.insert("acos".to_string(), unary_fn(f64::acos));
    function_map.insert("acosh".to_string(), unary_fn(f64::acosh));
    function_map.insert("add_months".to_string(), binary_fn(add_months));
    // aes_decrypt
    // aes_encrypt
    // aggregate
    // any
    // approx_count_distinct
    // approx_percentile
    function_map.insert("array".to_string(), Box::new(MakeArray));
    // array_agg
    function_map.insert("array_contains".to_string(), binary_fn(array_contains));
    function_map.insert("array_distinct".to_string(), unary_fn(array_distinct));
    function_map.insert("array_except".to_string(), binary_fn(array_except));
    function_map.insert("array_intersect".to_string(), binary_fn(array_intersect));
    function_map.insert("array_join".to_string(), Box::new(ArrayJoin));
    function_map.insert("array_max".to_string(), unary_fn(array_max));
    function_map.insert("array_min".to_string(), unary_fn(array_min));
    function_map.insert("array_position".to_string(), binary_fn(array_position));
    function_map.insert("array_remove".to_string(), binary_fn(array_remove));
    function_map.insert("array_repeat".to_string(), binary_fn(array_repeat));
    function_map.insert("array_size".to_string(), unary_fn(array_size));
    function_map.insert("array_union".to_string(), binary_fn(array_union));
    function_map.insert("arrays_overlap".to_string(), binary_fn(arrays_overlap));
    function_map.insert("arrays_zip".to_string(), binary_fn(arrays_zip));
    function_map.insert("ascii".to_string(), unary_fn(ascii));
    function_map.insert("asin".to_string(), unary_fn(f64::asin));
    function_map.insert("asinh".to_string(), unary_fn(f64::asinh));
    // assert_true
    function_map.insert("atan".to_string(), unary_fn(f64::atan));
    function_map.insert("atan2".to_string(), binary_fn(f64::atan2));
    function_map.insert("atanh".to_string(), unary_fn(f64::atanh));
    // avg
    // base64
    // between
    function_map.insert("bigint".to_string(), Box::new(TypeConverterFunction {to: ValueType::Long}));
    // bin
    // binary
    function_map.insert("bit_and".to_string(), var_fn(|v: Vec<u64>| v.iter().fold(0, |acc, x| acc & x)));
    function_map.insert("bit_count".to_string(), unary_fn(u64::count_ones));
    function_map.insert("bit_get".to_string(), binary_fn(|x: u64, y: u64| (x >> y) & 1));
    // bit_length
    function_map.insert("bit_not".to_string(), unary_fn(|x: u64| !x));
    function_map.insert("bit_or".to_string(), var_fn(|v: Vec<u64>| v.iter().fold(0, |acc, x| acc | x)));
    function_map.insert("bit_xor".to_string(), var_fn(|v: Vec<u64>| v.iter().fold(0, |acc, x| acc ^ x)));
    // bool_and
    // bool_or
    function_map.insert("boolean".to_string(), Box::new(TypeConverterFunction {to: ValueType::Bool}));
    // function_map.insert("bround".to_string(), binary_fn(|x: f64, y: i64| bround(x, y)));
    function_map.insert("btrim".to_string(), unary_fn(|x: String| x.trim().to_string()));
    // cardinality
    // case, implemented in syntax
    // cast, this needs special syntax
    function_map.insert("cbrt".to_string(), unary_fn(f64::cbrt));
    function_map.insert("ceil".to_string(), unary_fn(f64::ceil));
    // ceiling
    function_map.insert("char".to_string(), unary_fn(|x: i64| char::from_u32((x % 256) as u32).unwrap().to_string()));
    function_map.insert("char_length".to_string(), unary_fn(|s: String| s.chars().count() as i64));
    function_map.insert("character_length".to_string(), unary_fn(|s: String| s.chars().count() as i64));
    function_map.insert("chr".to_string(), unary_fn(|x: i64| char::from_u32((x % 256) as u32).unwrap().to_string()));
    function_map.insert("coalesce".to_string(), var_fn(|args: Vec<Value>| args.into_iter().find(|v| !v.is_null()).unwrap_or(Value::Null)));
    // collect_list
    // collect_set
    function_map.insert("concat".to_string(), Box::new(Concat));
    function_map.insert("concat_ws".to_string(), Box::new(ConcatWs));
    function_map.insert("contains".to_string(), binary_fn(contains));
    function_map.insert("conv".to_string(), Box::new(Conv));
    // corr
    function_map.insert("cos".to_string(), unary_fn(f64::cos));
    function_map.insert("cosh".to_string(), unary_fn(f64::cosh));
    function_map.insert("cot".to_string(), unary_fn(|x| 1.0 / f64::tan(x)));
    // count
    // count_if
    // count_min_sketch
    // covar_pop
    // covar_samp
    // crc32
    function_map.insert("csc".to_string(), unary_fn(|x| 1.0 / f64::sin(x)));
    // cume_dist
    // current_catalog
    // current_database
    function_map.insert("csc".to_string(), unary_fn(|x| 1.0 / f64::sin(x)));
    function_map.insert("current_date".to_string(), nullary_fn(|| Utc::now().date_naive()));
    function_map.insert("current_timestamp".to_string(), nullary_fn(Utc::now));
    // current_timezone
    // current_user
    function_map.insert("date".to_string(), Box::new(TypeConverterFunction {to: ValueType::DateTime}));
    function_map.insert("date_add".to_string(), binary_fn(add_days));
    // * date_format, need to figure out how to handle Spark format string
    function_map.insert("date_from_unix_date".to_string(), unary_fn(|x: i32| NaiveDate::from_num_days_from_ce_opt(x).unwrap()));
    // * date_part
    function_map.insert("date_sub".to_string(), binary_fn(|d: NaiveDate, n: i64| add_days(d, -n)));
    // * date_trunc
    // * date_diff
    function_map.insert("day".to_string(), unary_fn(|d: NaiveDate| d.day()));
    // * dayofmonth
    // * dayofweek
    // * dayofyear
    // decimal
    // decode
    function_map.insert("degrees".to_string(), unary_fn(|x: f64| x * 180.0 / std::f64::consts::PI));
    // dense_rank
    // * div, operator
    function_map.insert("double".to_string(), Box::new(TypeConverterFunction {to: ValueType::Double}));
    // * e, we have E, need change parser
    // element_at
    // elt
    // encode
    function_map.insert("endswith".to_string(), binary_fn(|s: String, sub: String| s.ends_with(&sub)));
    // every
    // exists
    function_map.insert("exp".to_string(), unary_fn(f64::exp));
    // explode
    // explode_outer
    function_map.insert("expm1".to_string(), unary_fn(f64::exp_m1));
    // extract
    // factorial
    // filter
    // find_in_set
    // first
    // first_value
    // flatten
    function_map.insert("float".to_string(), Box::new(TypeConverterFunction {to: ValueType::Float}));
    function_map.insert("floor".to_string(), unary_fn(f64::floor));
    // forall
    // format_number
    // format_string
    // from_csv
    // from_json
    // from_unixtime
    // from_utc_timestamp
    function_map.insert("get_json_array".to_string(), Box::new(ExtractJsonArray));  // Added
    function_map.insert("get_json_object".to_string(), Box::new(ExtractJsonObject));
    function_map.insert("getbit".to_string(), binary_fn(|x: u64, y: u64| (x >> y) & 1));
    // greatest
    // grouping
    // grouping_id
    // hash
    // hex
    // histogram_numeric
    function_map.insert("hour".to_string(), unary_fn(|t: NaiveDateTime| t.hour()));
    // hypot
    // if
    function_map.insert("ifnull".to_string(), binary_fn(|x: Value, y: Value| if x.is_null() {y} else {x}));
    // ilike
    // in
    // initcap
    // inline
    // inline_outer
    // input_file_block_length
    // input_file_block_start
    // input_file_name
    function_map.insert("instr".to_string(), binary_fn(|s: String, sub: String| s.find(&sub).map(|x| x + 1).unwrap_or(0))); // 1-based
    function_map.insert("int".to_string(), Box::new(TypeConverterFunction {to: ValueType::Int}));
    function_map.insert("isnan".to_string(), unary_fn(f64::is_nan));
    function_map.insert("isnotnull".to_string(), unary_fn(|v: Value| !v.is_null()));
    function_map.insert("isnull".to_string(), unary_fn(|v: Value| v.is_null()));
    // java_method
    // json_array_length
    // json_object_keys
    // json_tuple
    // kurtosis
    // lag
    // last
    function_map.insert("last_day".to_string(), unary_fn(|v: NaiveDate| v - Duration::days(1)));
    // last_value
    // lcase
    // lead
    // least
    // left
    function_map.insert("length".to_string(), Box::new(Len));  // Added
    function_map.insert("levenshtein".to_string(), binary_fn(|a: String, b: String| levenshtein::levenshtein(&a, &b)));
    // like
    function_map.insert("ln".to_string(), unary_fn(f64::ln));
    // locate
    function_map.insert("log".to_string(), binary_fn(f64::log));
    function_map.insert("log10".to_string(), unary_fn(f64::log10));
    function_map.insert("log1p".to_string(), unary_fn(f64::ln_1p));
    function_map.insert("log2".to_string(), unary_fn(f64::log2));
    function_map.insert("lower".to_string(), unary_fn(|s: String| s.to_lowercase()));
    // lpad
    function_map.insert("ltrim".to_string(), unary_fn(|s: String| s.trim_start().to_string()));
    // make_date
    // make_dt_interval
    // make_interval
    // make_timestamp
    // make_ym_interval
    // map
    // map_concat
    // map_contains_key
    // map_entries
    // map_filter
    // map_from_arrays
    // map_from_entries
    // map_keys
    // map_values
    // map_zip_with
    // max
    // max_by
    // md5
    // mean
    // min
    // min_by
    function_map.insert("minute".to_string(), unary_fn(|t: NaiveDateTime| t.minute()));
    // mod
    function_map.insert("mod".to_string(), binary_fn(f64::rem_euclid));
    // monotonically_increasing_id
    function_map.insert("month".to_string(), unary_fn(|d: NaiveDate| d.month()));
    // months_between
    // named_struct
    // nanvl
    // negative
    // next_day
    // not
    // now
    function_map.insert("now".to_string(), nullary_fn(Utc::now));
    // nth_value
    // ntile
    function_map.insert("nullif".to_string(), binary_fn(|x: Value, y: Value| if x == y { Value::Null } else { x }));
    function_map.insert("nvl".to_string(), binary_fn(|x: Value, y: Value| if x.is_null() { y } else { x }));
    // nvl2
    // octet_length
    // or
    // overlay
    // parse_url
    // percent_rank
    // percentile
    // percentile_approx
    // pi   // we have PI, need change parser
    // pmod
    // posexplode
    // posexplode_outer
    // position
    // positive
    function_map.insert("pow".to_string(), binary_fn(f64::powf));
    function_map.insert("power".to_string(), binary_fn(f64::powf));
    // printf
    function_map.insert("quarter".to_string(), unary_fn(quarter));
    function_map.insert("radians".to_string(), unary_fn(|v: f64| v * std::f64::consts::PI / 180.0));
    // raise_error
    function_map.insert("rand".to_string(), nullary_fn(rand_functions::rand));
    // randn
    function_map.insert("random".to_string(), nullary_fn(rand_functions::rand));
    // rank
    // reflect
    // regexp
    // regexp_extract
    // regexp_extract_all
    // regexp_like
    // regexp_replace
    // regr_avgx
    // regr_avgy
    // regr_count
    // regr_r2
    // repeat
    // replace
    // reverse
    // right
    // rint
    // rlike
    function_map.insert("round".to_string(), unary_fn(f64::round));
    // row_number
    // rpad
    function_map.insert("rtrim".to_string(), unary_fn(|s: String| s.trim_end().to_string()));
    // schema_of_csv
    // schema_of_json
    function_map.insert("sec".to_string(), unary_fn(|x: f64| 1.0 / x.cos()));
    function_map.insert("second".to_string(), unary_fn(|t: NaiveDateTime| t.second()));
    // sentences
    // sequence
    // session_window
    // sha
    // sha1
    // sha2
    // shiftleft
    // shiftright
    // shiftrightunsigned
    // shuffle
    function_map.insert("shuffle".to_string(), unary_fn(rand_functions::shuffle));
    // sign
    // signum
    function_map.insert("sin".to_string(), unary_fn(f64::sin));
    function_map.insert("sinh".to_string(), unary_fn(f64::sinh));
    function_map.insert("size".to_string(), unary_fn(|v: Vec<Value>| v.len()));
    // skewness
    // slice
    // smallint
    // some
    // sort_array
    // soundex
    function_map.insert("space".to_string(), unary_fn(|n: usize| " ".repeat(n)));
    // spark_partition_id
    // split
    // split_part
    function_map.insert("sqrt".to_string(), unary_fn(f64::sqrt));
    // stack
    // startswith
    // std
    // stddev
    // stddev_pop
    // stddev_samp
    // str_to_map
    function_map.insert("string".to_string(), Box::new(TypeConverterFunction { to: ValueType::String }));
    // struct
    function_map.insert("substring".to_string(), Box::new(SubstringFunction));
    function_map.insert("substring_index".to_string(), ternary_fn(substring::substring_index));
    // sum
    function_map.insert("tan".to_string(), unary_fn(f64::tan));
    function_map.insert("tanh".to_string(), unary_fn(f64::tanh));
    // timestamp
    // timestamp_micros
    // timestamp_millis
    // timestamp_seconds
    // tinyint
    // to_binary
    // to_csv
    // to_date
    function_map.insert("to_json".to_string(), Box::new(ToJsonStringFunction));
    // to_number
    // to_timestamp
    function_map.insert("to_unix_timestamp".to_string(), Box::new(TimestampFunction));  // TODO: support Java format
    // to_utc_timestamp
    // transform
    // transform_keys
    // transform_values
    // translate
    function_map.insert("trim".to_string(), unary_fn(|s: String| s.trim().to_string()));
    // trunc
    // try_add
    // try_avg
    // try_divide
    // try_element_at
    // try_multiply
    // try_subtract
    // try_sum
    // try_to_binary
    // try_to_number
    // typeof
    function_map.insert("ucase".to_string(), unary_fn(|s: String| s.to_uppercase()));
    // unbase64
    // unhex
    // unix_date
    // unix_micros
    // unix_millis
    // unix_seconds
    function_map.insert("unix_timestamp".to_string(), Box::new(TimestampFunction));  // TODO: support Java format
    function_map.insert("upper".to_string(), unary_fn(|s: String| s.to_uppercase()));
    function_map.insert("uuid".to_string(), nullary_fn(|| uuid::Uuid::new_v4().to_string()));
    // var_pop
    // var_samp
    // variance
    // version
    function_map.insert("weekday".to_string(), unary_fn(|t: NaiveDate| t.weekday() as usize));
    // weekofyear
    // when
    // width_bucket
    // window
    // xpath
    // xpath_boolean
    // xpath_double
    // xpath_float
    // xpath_int
    // xpath_long
    // xpath_number
    // xpath_short
    // xpath_string
    // xxhash64
    // year
    function_map.insert("year".to_string(), unary_fn(|s: NaiveDate| s.year()));
    // zip_with
    



    function_map.insert("split".to_string(), Box::new(SplitFunction));
    function_map.insert("case".to_string(), Box::new(CaseFunction));
    function_map.insert("bucket".to_string(), Box::new(BucketFunction));
    function_map.insert("timestamp".to_string(), Box::new(TimestampFunction));
    function_map.insert("len".to_string(), Box::new(Len));

    function_map
}
