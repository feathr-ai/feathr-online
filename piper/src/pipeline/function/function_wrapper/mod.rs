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
```rust,ignore
 trait IntoFunction {
    fn into_function(self) -> Box<dyn Function>;
}

// This compiles
impl<R, F> IntoFunction for F
where
   Self: Fn()->R + Sync + Send + Clone + 'static,
   R: Into<Value> + Sync + Send + ValueTypeOf + Clone + 'static,
{
   fn into_function(self) -> Box<dyn Function> {
       nullary::nullary_fn(self)
   }
}

// But this doesn't because `E` is not directly bounded by `Self: Fn(A)->R + ...`
impl<R, F, A, E> IntoFunction for F
where
   Self: Fn(A)->R + Sync + Send + Clone + 'static,
   R: Into<Value> + Sync + Send + ValueTypeOf + Clone + 'static,
   A: Send + Sync + Clone + TryFrom<Value, Error = E>,
   Result<Value, E>: Into<Value>,
   E: Sync + Send + Clone,
{
   fn into_function(self) -> Box<dyn Function> {
       unary::unary_fn(self)
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
