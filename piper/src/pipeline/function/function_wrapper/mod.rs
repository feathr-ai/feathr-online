use super::Function;

mod binary;
mod nullary;
mod quaternary;
mod ternary;
mod unary;
mod variadic;

/**
 * NOTE: Seems to make generic trait over all function wrappers needs HRTB (and GAT?).
 *
 * E.g. following code does not compile:
 ``` ignore
 trait IntoFunction {
     fn into_function(self) -> Box<dyn Function>;
 }

impl<R, F> IntoFunction for F
where
    Self: Fn()->R + Sync + Send
    R: Into<Value> + Sync + Send + ValueTypeOf,
{
    fn into_function(self) -> Box<dyn Function> {
        nullary::nullary_fn(self)
    }
}
 ```
 */

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
 * Wrap a quaternary function into `Function` so it can be registered in the function registry.
 */
pub use quaternary::quaternary_fn;

/**
 * Wrap a unary function into `Function` so it can be registered in the function registry.
 */
pub use unary::unary_fn;

/**
 * Wrap a variadic function into `Function` so it can be registered in the function registry.
 */
pub use variadic::var_fn;
