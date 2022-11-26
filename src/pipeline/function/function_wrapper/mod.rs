use super::Function;

mod binary;
mod nullary;
mod ternary;
mod unary;
mod variadic;

/**
 * Wrap a binary function into `Function` so it can be registered in the function registry.
 */
pub use binary::binary_fn;

/**
 * Wrap a nullary function into `Function` so it can be registered in the function registry.
 */
pub use nullary::nullary_fn;

/**
 * Wrap a ternary function into `Function` so it can be registered in the function registry.
 */
pub use ternary::ternary_fn;

/**
 * Wrap a unary function into `Function` so it can be registered in the function registry.
 */
pub use unary::unary_fn;

/**
 * Wrap a variadic function into `Function` so it can be registered in the function registry.
 */
pub use variadic::var_fn;
