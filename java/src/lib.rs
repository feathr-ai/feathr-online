use std::collections::HashMap;

use jni::JNIEnv;

use jni::objects::{JClass, JMap, JObject, JString};

use jni::signature::ReturnType;
use jni::sys::{jlong, jshort};
use piper::{PiperError, PiperService};
use tokio::runtime::Handle;

mod java_function;
mod jvm_cache;
mod value_utils;

pub use jvm_cache::get_jvm;
pub use value_utils::{to_jvalue, to_value};

#[no_mangle]
pub extern "system" fn Java_com_github_windoze_feathr_piper_PiperService_create(
    env: JNIEnv,
    _class: JClass,
    pipelines: JString,
    lookups: JString,
    functions: JObject,
) -> jlong {
    jvm_cache::set_jvm(&env);
    if pipelines.is_null() {
        illegal_argument(&env, "pipelines is null");
        return 0;
    }
    let pipelines: String = match env.get_string(pipelines) {
        Ok(v) => v,
        Err(e) => {
            println!("Error getting pipelines: {e}");
            illegal_argument(&env, &e.to_string());
            return 0;
        }
    }
    .into();
    let lookups: String = if lookups.is_null() {
        Default::default()
    } else {
        match env.get_string(lookups) {
            Ok(v) => v,
            Err(e) => {
                illegal_argument(&env, &e.to_string());
                return 0;
            }
        }
        .into()
    };

    let udf: HashMap<_, _> = if functions.is_null() {
        Default::default()
    } else {
        match env.is_instance_of(functions, &get_jvm().map_cls) {
            Ok(true) => {}
            Ok(false) => {
                illegal_argument(&env, "functions is not a map");
                return 0;
            }
            Err(e) => {
                illegal_argument(&env, &e.to_string());
                return 0;
            }
        };

        let map: JMap = match JMap::from_env(&env, functions) {
            Ok(v) => v,
            Err(e) => {
                illegal_argument(&env, &e.to_string());
                return 0;
            }
        };

        match map.iter() {
            Ok(v) => v,
            Err(e) => {
                illegal_argument(&env, &e.to_string());
                return 0;
            }
        }
        .map(|(k, v)| {
            let k: String = env.get_string(k.into()).unwrap().into();
            let v = Box::new(java_function::JavaFunction::new(&env, v).unwrap())
                as Box<dyn piper::Function>;
            (k, v)
        })
        .collect()
    };

    Box::into_raw(Box::new(piper::PiperService::create(
        &pipelines, &lookups, udf,
    ))) as jlong
}

/**
 * # Safety
 *
 * unsafe because it dereferences a raw pointer as JNI spec required
 */
#[no_mangle]
pub unsafe extern "system" fn Java_com_github_windoze_feathr_piper_PiperService_start(
    env: JNIEnv,
    _class: JClass,
    svc_handle: jlong,
    address: JString,
    port: jshort,
) {
    let mut svc = Box::from_raw(svc_handle as *mut PiperService);
    let address: String = match env.get_string(address) {
        Ok(v) => v,
        Err(e) => {
            illegal_argument(&env, &e.to_string());
            return;
        }
    }
    .into();
    let port: u16 = port as u16;
    let ret = block_on(async move { svc.start_at(&address, port).await });
    match ret {
        Ok(_) => {}
        Err(e) => {
            if matches!(e, PiperError::Interrupted) {
                // ignore interrupted error because it's caused by `stop()` method
                return;
            }
            // Throw exception otherwise
            runtime_exception(&env, &e.to_string());
        }
    }
}

/**
 * # Safety
 *
 * unsafe because it dereferences a raw pointer as JNI spec required
 */
#[no_mangle]
pub unsafe extern "system" fn Java_com_github_windoze_feathr_piper_PiperService_stop(
    _env: JNIEnv,
    _class: JClass,
    svc_handle: jlong,
) {
    let svc = &mut *(svc_handle as *mut PiperService);
    svc.stop();
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match Handle::try_current() {
        Ok(handle) => handle.block_on(future),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future),
    }
}

fn runtime_exception(env: &JNIEnv, msg: &str) {
    let cls = &get_jvm().runtime_exception_cls;
    env.throw_new(cls, msg).unwrap();
}

fn illegal_argument(env: &JNIEnv, msg: &str) {
    let illegal_argument_exception_cls = &get_jvm().illegal_argument_exception_cls;
    env.throw_new(illegal_argument_exception_cls, msg).unwrap();
}

pub fn to_string(env: &JNIEnv, obj: JObject) -> String {
    let to_string_method = get_jvm().to_string;
    let ret = env.call_method_unchecked(obj, to_string_method, ReturnType::Object, &[]);
    match ret {
        Ok(v) => env.get_string(v.l().unwrap().into()).unwrap().into(),
        Err(e) => {
            println!("{e:?}");
            e.to_string()
        }
    }
}
