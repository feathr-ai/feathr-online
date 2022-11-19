use std::fmt::{Debug, Display};

use tracing::error;

/// Log if the result is an error
pub trait Logged {
    fn log(self) -> Self;
}

impl<T: Sized, E: Display> Logged for Result<T, E> {
    fn log(self) -> Self {
        match &self {
            Ok(_) => {}
            Err(e) => error!("{}", e),
        }
        self
    }
}

/// Call a function by using the object as the receiver.
/// e.g. show some logs when an expression has been evaluated
pub trait Appliable
where
    Self: Sized,
{
    /// Call function that may mutate the state of `self`
    fn apply<F>(self, f: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        f(self)
    }

    /// Call function that doesn't mutate the state of `self`
    fn then<F>(self, f: F) -> Self
    where
        F: FnOnce(&Self),
    {
        f(&self);
        self
    }
}

/// Every sized type can be applied
impl<T> Appliable for T where T: Sized {}

/// Ignore field from `Debug` auto trait
pub struct IgnoreDebug<T> {
    pub inner: T,
}

impl<T> Debug for IgnoreDebug<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("...")
    }
}

// Implementation of most common auto traits

impl<T> Copy for IgnoreDebug<T> where T: Copy {}

impl<T> Clone for IgnoreDebug<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for IgnoreDebug<T>
where
    T: Default
{
    fn default() -> Self {
        Self {
            inner: T::default(),
        }
    }
}
