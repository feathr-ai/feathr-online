use thiserror::Error;

use super::ValueType;

// All errors that can be returned by the pipeline.
#[derive(Clone, Debug, Error)]
pub enum PiperError {
    // Unknown error
    #[error("{0}")]
    Unknown(String),

    #[error("Pipeline {0} is not found")]
    PipelineNotFound(String),

    // DSL syntax errors
    #[error("{0}")]
    SyntaxError(String),

    #[error("{0}")]
    ValidationError(String),

    // Row has incorrect number of fields
    #[error("Expecting row with {1} columns, but got {0}")]
    InvalidRowLength(usize, usize),

    // Field in row has incorrect type
    #[error("Expecting column {0} to be {1}, but got {2}")]
    InvalidColumnType(String, ValueType, ValueType),

    // Type cast failed
    #[error("Cannot cast from type {0:?} to type {1:?}.")]
    InvalidTypeCast(ValueType, ValueType),

    // Type conversion failed
    #[error("Cannot convert from type {0:?} to type {1:?}.")]
    InvalidTypeConversion(ValueType, ValueType),

    // Arguments with given types cannot be applied to the operator, e.g. string + array
    #[error("Cannot apply '{0}' operation between {1:?} and {2:?}.")]
    TypeMismatch(String, ValueType, ValueType),

    // Unary operator got invalid argument type
    #[error("Cannot apply '{0}' operation to {1:?}.")]
    InvalidOperandType(String, ValueType),

    // Value is not in the expected type
    #[error("Assume value is {1:?}, but actual type is {0:?}.")]
    InvalidValueType(ValueType, ValueType),

    // Function has incorrect type of argument
    #[error("Invalid type {2} of argument {1} for function {0}.")]
    InvalidArgumentType(String, usize, ValueType),

    // Function has incorrect type of argument
    #[error("{0}")]
    InvalidValue(String),

    // Function has incorrect number of arguments
    #[error("Invalid argument count, expecting {0}, got {1}.")]
    InvalidArgumentCount(usize, usize),

    // Variadic function has invalid number of arguments
    #[error("{0} cannot take {1} arguments.")]
    ArityError(String, usize),

    // Column is not found
    #[error("Column '{0}' not found.")]
    ColumnNotFound(String),

    // String format error
    #[error("String {0} is not a valid {1:?}.")]
    FormatError(String, ValueType),

    // Unrecognized operator
    #[error("Unknown operator {0}.")]
    UnknownOperator(String),

    // Unknown function
    #[error("Unknown function {0}.")]
    UnknownFunction(String),

    // Unknown lookup source
    #[error("Lookup data source '{0}' not found.")]
    LookupSourceNotFound(String),

    // HTTP Lookup Source specific

    // Invalid HTTP method
    #[error("Invalid method {0}")]
    InvalidMethod(String),

    // Invalid JSON path
    #[error("Invalid JSON string {0}")]
    InvalidJsonString(String),

    // Invalid JSON path
    #[error("Invalid JSONPath {0}")]
    InvalidJsonPath(String),

    // Unspecific HTTP error
    #[error("{0}")]
    AuthError(String),

    // Unspecific HTTP error
    #[error("{0}")]
    HttpError(String),

    // Feathr Online Store specific

    // Redis error
    #[error("{0}")]
    RedisError(String),

    // Base64 decoding error
    #[error("{0}")]
    Base64Error(String),

    // Protobuf decoding error
    #[error("{0}")]
    ProtobufError(String),

    #[error("Environment variable {0} is not set.")]
    EnvVarNotSet(String),

    #[error("The service has been stopped.")]
    Interrupted,

    #[error("{0}")]
    ExternalError(String),

    #[error("Column with name {0} already exists.")]
    ColumnAlreadyExists(String),

    #[error("Function with name {0} already exists.")]
    FunctionAlreadyDefined(String),
}
