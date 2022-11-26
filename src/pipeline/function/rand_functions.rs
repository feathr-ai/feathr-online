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

pub fn rand() -> f64 {
    RandomGen::RNG.with(|rng| rng.borrow_mut().gen())
}

pub fn shuffle(mut array: Vec<Value>) -> Vec<Value> {
    RandomGen::RNG.with(|rng| {
        let r: &mut ThreadRng = &mut rng.borrow_mut();
        array.shuffle(r)
    });
    array
}
