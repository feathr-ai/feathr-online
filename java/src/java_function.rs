use jni::{
    objects::{GlobalRef, JList, JMethodID, JObject, JValue},
    sys::jvalue,
    JNIEnv,
};

use crate::{get_jvm, to_jvalue, to_string, to_value, MAX_ARITY};

#[derive(Clone)]
pub struct JavaFunction {
    pub obj: GlobalRef,
    pub method_id: JMethodID,
    pub arity: usize,
}

impl JavaFunction {
    pub fn new(env: &JNIEnv, obj: JObject) -> Result<Self, piper::PiperError> {
        let obj = env.new_global_ref(obj).unwrap();

        // Check from Function0 to Function'MAX_ARITY'
        for a in 0..MAX_ARITY {
            let cls = env
                .find_class(format!("com/azure/feathr/piper/Function{}", a))
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
            if env
                .is_instance_of(&obj, cls)
                .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?
            {
                let method_id = env
                    .get_method_id(
                        cls,
                        format!("apply{}", a),
                        format!("({})Ljava/lang/Object;", "Ljava/lang/Object;".repeat(a)),
                    )
                    .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
                return Ok(Self {
                    obj,
                    method_id,
                    arity: a,
                });
            }
        }

        // Assume VarFunction
        let cls = env
            .find_class("com/azure/feathr/piper/VarFunction")
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        let method_id = env
            .get_method_id(cls, "applyVar", "([Ljava/lang/Object;)Ljava/lang/Object;")
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))?;
        Ok(Self {
            obj,
            method_id,
            arity: MAX_ARITY,
        })
    }
}

impl piper::Function for JavaFunction {
    fn get_output_type(
        &self,
        _argument_types: &[piper::ValueType],
    ) -> Result<piper::ValueType, piper::PiperError> {
        Ok(piper::ValueType::Dynamic)
    }

    fn eval(&self, arguments: Vec<piper::Value>) -> piper::Value {
        let env = match get_jvm().jvm.attach_current_thread_as_daemon() {
            Ok(env) => env,
            Err(e) => return piper::Value::Error(piper::PiperError::ExternalError(e.to_string())),
        };

        let args: Vec<jvalue> = if self.arity == MAX_ARITY {
            // call applyVar
            let array_list_cls = &get_jvm().array_list_cls;
            let new_array_list = get_jvm().new_array_list;
            let l = env
                .new_object_unchecked(array_list_cls, new_array_list, &[])
                .unwrap();
            let j = JList::from_env(&env, l).unwrap();
            for arg in arguments {
                j.add(to_jvalue(arg, &env)).unwrap();
            }
            let o: JObject = j.into();
            vec![JValue::Object(o).to_jni()]
        } else {
            arguments
                .into_iter()
                .take(self.arity)
                .map(|a| JValue::Object(to_jvalue(a, &env)).to_jni())
                .collect()
        };

        let ret = env
            .call_method_unchecked(
                self.obj.as_obj(),
                self.method_id,
                jni::signature::ReturnType::Object,
                &args,
            )
            .map_err(|e| piper::PiperError::ExternalError(e.to_string()))
            .and_then(|v| {
                to_value(v, &env).map_err(|e| piper::PiperError::ExternalError(e.to_string()))
            });
        if ret.is_err() {
            match env.exception_occurred() {
                Ok(ex) => {
                    if !ex.is_null() {
                        env.exception_clear().unwrap();
                        return piper::PiperError::ExternalError(to_string(&env, ex.into())).into();
                    }
                }
                Err(e) => return piper::PiperError::ExternalError(e.to_string()).into(),
            };
        }
        ret.into()
    }
}
