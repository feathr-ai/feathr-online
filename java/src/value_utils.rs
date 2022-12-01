use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use jni::{
    objects::{JList, JMap, JObject, JValue},
    signature::{Primitive, ReturnType},
    JNIEnv,
};

use crate::get_jvm;

pub fn to_jvalue<'a>(value: piper::Value, env: &JNIEnv<'a>) -> JObject<'a> {
    let jvm = get_jvm();
    // All below `unwrap()` are not supposed to fail
    match value {
        piper::Value::Null => JObject::null(),
        piper::Value::Bool(v) => env
            .new_object_unchecked(&jvm.bool_cls, jvm.new_bool, &[v.into()])
            .unwrap(),
        piper::Value::Int(v) => env
            .new_object_unchecked(&jvm.int_cls, jvm.new_int, &[v.into()])
            .unwrap(),
        piper::Value::Long(v) => env
            .new_object_unchecked(&jvm.long_cls, jvm.new_long, &[v.into()])
            .unwrap(),
        piper::Value::Float(v) => env
            .new_object_unchecked(&jvm.float_cls, jvm.new_float, &[v.into()])
            .unwrap(),
        piper::Value::Double(v) => env
            .new_object_unchecked(&jvm.double_cls, jvm.new_double, &[v.into()])
            .unwrap(),
        piper::Value::String(v) => env.new_string(v).unwrap().into(),
        piper::Value::Array(array) => {
            let array_list_cls = &get_jvm().array_list_cls;
            let new_array_list = get_jvm().new_array_list;
            let o = env
                .new_object_unchecked(array_list_cls, new_array_list, &[])
                .unwrap();
            let l = JList::from_env(env, o).unwrap();
            for v in array {
                l.add(to_jvalue(v, env)).unwrap();
            }
            l.into()
        }
        piper::Value::Object(map) => {
            let hash_map_cls = &get_jvm().hash_map_cls;
            let new_hash_map = get_jvm().new_hash_map;
            let o = env
                .new_object_unchecked(hash_map_cls, new_hash_map, &[])
                .unwrap();
            let m = JMap::from_env(env, o).unwrap();
            for (k, v) in map {
                let k = env.new_string(k).unwrap();
                let v = to_jvalue(v, env);
                m.put(k.into(), v).unwrap();
            }
            m.into()
        }
        piper::Value::DateTime(v) => {
            let sec = v.timestamp() as i64;
            let ns = v.timestamp_subsec_nanos() as i64;
            let instant_cls = &get_jvm().instant_cls;
            let new_instant = get_jvm().new_instant;
            let args = [JValue::Long(sec).into(), JValue::Long(ns).into()];
            env.call_static_method_unchecked(instant_cls, new_instant, ReturnType::Object, &args)
                .unwrap();
            todo!()
        }
        piper::Value::Error(_) => JObject::null(),
    }
}

pub fn to_value<'a>(o: JValue<'a>, env: &JNIEnv<'a>) -> Result<piper::Value, piper::PiperError> {
    if let Ok(v) = o.l() {
        return obj_to_value(v, env);
    }

    Err(piper::PiperError::ExternalError(
        "Unsupported value type".to_string(),
    ))
}

fn obj_to_value<'a>(obj: JObject<'a>, env: &JNIEnv<'a>) -> Result<piper::Value, piper::PiperError> {
    let state = get_jvm();

    if obj.is_null() {
        return Ok(piper::Value::Null);
    }

    if env
        .is_instance_of(obj, &state.bool_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let v = env
            .call_method_unchecked(
                obj,
                state.get_bool_value,
                ReturnType::Primitive(Primitive::Boolean),
                &[],
            )
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        return Ok(piper::Value::Bool(
            v.z()
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?,
        ));
    }

    if env
        .is_instance_of(obj, &state.int_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let v = env
            .call_method_unchecked(
                obj,
                state.get_int_value,
                ReturnType::Primitive(Primitive::Int),
                &[],
            )
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        return Ok(piper::Value::Int(
            v.i()
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?,
        ));
    }

    if env
        .is_instance_of(obj, &state.long_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let v = env
            .call_method_unchecked(
                obj,
                state.get_long_value,
                ReturnType::Primitive(Primitive::Long),
                &[],
            )
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        return Ok(piper::Value::Long(
            v.j()
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?,
        ));
    }

    if env.is_instance_of(obj, &state.float_cls).unwrap() {
        let v = env
            .call_method_unchecked(
                obj,
                state.get_float_value,
                ReturnType::Primitive(Primitive::Float),
                &[],
            )
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        return Ok(piper::Value::Float(
            v.f()
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?,
        ));
    }

    if env
        .is_instance_of(obj, &state.double_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let v = env
            .call_method_unchecked(
                obj,
                state.get_double_value,
                ReturnType::Primitive(Primitive::Double),
                &[],
            )
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        return Ok(piper::Value::Double(
            v.d()
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?,
        ));
    }

    if env
        .is_instance_of(obj, &state.string_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let s: String = env
            .get_string(obj.into())
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
            .into();
        return Ok(piper::Value::String(s.into()));
    }

    if env
        .is_instance_of(obj, &state.instant_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let ts = env
            .call_method_unchecked(
                obj,
                state.get_epoch_second,
                ReturnType::Primitive(Primitive::Long),
                &[],
            )
            .and_then(|v| v.j())
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        let ns = env
            .call_method_unchecked(
                obj,
                state.get_nano,
                ReturnType::Primitive(Primitive::Long),
                &[],
            )
            .and_then(|v| v.j())
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        let dt: DateTime<Utc> = Utc.timestamp_opt(ts, ns as u32).unwrap();
        return Ok(piper::Value::DateTime(dt));
    }

    if env
        .is_instance_of(obj, &state.list_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let l = JList::from_env(env, obj)
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        let v: piper::Value = l
            .iter()
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
            .map(|v| obj_to_value(v, env))
            .collect();
        return Ok(v);
    }

    if env
        .is_instance_of(obj, &state.map_cls)
        .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
    {
        let l = JMap::from_env(env, obj)
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        let mut m = HashMap::new();
        for (k, v) in l
            .iter()
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
        {
            if !env
                .is_instance_of(k, &state.string_cls)
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
            {
                return Err(piper::PiperError::ExternalError(
                    "Map keys must be strings".to_string(),
                ));
            }
            let k: String = env
                .get_string(k.into())
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
                .into();
            let v = obj_to_value(v, env)?;
            m.insert(k, v);
        }
        return Ok(piper::Value::Object(m));
    }

    Err(piper::PiperError::ExternalError(
        "Unsupported value type".to_string(),
    ))
}
