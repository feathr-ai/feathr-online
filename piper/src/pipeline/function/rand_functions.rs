use std::cell::RefCell;

use rand::{rngs::ThreadRng, seq::SliceRandom, Rng};

use crate::pipeline::Value;

/**
 * All random functions share the same thread-local random number generator.
 */
struct RandomGen;

impl RandomGen {
    thread_local! {
        static RNG: RefCell<ThreadRng> = RefCell::new(rand::thread_rng());
    }
}

// `rand` uses HC128, no idea why Devskim reports it as insecure.
// Devskim: ignore DS148264
pub fn rand() -> f64 {
    RandomGen::RNG.with(|rng| rng.borrow_mut().gen())
}

// Devskim: ignore DS148264
pub fn shuffle(mut array: Vec<Value>) -> Vec<Value> {
    RandomGen::RNG.with(|rng| {
        let r: &mut ThreadRng = &mut rng.borrow_mut();
        // Devskim: ignore DS148264
        array.shuffle(r)
    });
    array
}
