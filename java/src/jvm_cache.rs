use jni::{
    objects::{GlobalRef, JMethodID, JStaticMethodID},
    JNIEnv, JavaVM,
};
use once_cell::sync::OnceCell;

pub struct JavaStates {
    pub jvm: JavaVM,

    pub obj_cls: GlobalRef,
    pub to_string: JMethodID,

    pub illegal_argument_exception_cls: GlobalRef,
    pub runtime_exception_cls: GlobalRef,

    pub array_list_cls: GlobalRef,
    pub new_array_list: JMethodID,

    pub hash_map_cls: GlobalRef,
    pub new_hash_map: JMethodID,

    pub bool_cls: GlobalRef,
    pub new_bool: JMethodID,
    pub get_bool_value: JMethodID,

    pub short_cls: GlobalRef,
    pub new_short: JMethodID,
    pub get_short_value: JMethodID,

    pub int_cls: GlobalRef,
    pub new_int: JMethodID,
    pub get_int_value: JMethodID,

    pub long_cls: GlobalRef,
    pub new_long: JMethodID,
    pub get_long_value: JMethodID,

    pub float_cls: GlobalRef,
    pub new_float: JMethodID,
    pub get_float_value: JMethodID,

    pub double_cls: GlobalRef,
    pub new_double: JMethodID,
    pub get_double_value: JMethodID,

    pub string_cls: GlobalRef,

    pub instant_cls: GlobalRef,
    pub new_instant: JStaticMethodID,
    pub get_epoch_second: JMethodID,
    pub get_nano: JMethodID,

    pub list_cls: GlobalRef,

    pub map_cls: GlobalRef,

    pub max_arity: usize,
}

pub static JVM: OnceCell<JavaStates> = OnceCell::new();

/**
 * One-off initialization, save classes and method for later use.
 */
pub fn set_jvm(env: &JNIEnv) {
    JVM.get_or_init(|| {
        let jvm = env.get_java_vm().unwrap();

        let obj_cls = env.find_class("java/lang/Object").unwrap();
        let to_string = env
            .get_method_id(obj_cls, "toString", "()Ljava/lang/String;")
            .unwrap();
        let obj_cls = env.new_global_ref(obj_cls).unwrap();

        let illegal_argument_exception_cls = env
            .find_class("java/lang/IllegalArgumentException")
            .unwrap();
        let illegal_argument_exception_cls =
            env.new_global_ref(illegal_argument_exception_cls).unwrap();

        let runtime_exception_cls = env.find_class("java/lang/RuntimeException").unwrap();
        let runtime_exception_cls = env.new_global_ref(runtime_exception_cls).unwrap();

        let array_list_cls = env.find_class("java/util/ArrayList").unwrap();
        let new_array_list = env.get_method_id(array_list_cls, "<init>", "()V").unwrap();
        let array_list_cls = env.new_global_ref(array_list_cls).unwrap();

        let hash_map_cls = env.find_class("java/util/ArrayList").unwrap();
        let new_hash_map = env.get_method_id(hash_map_cls, "<init>", "()V").unwrap();
        let hash_map_cls = env.new_global_ref(hash_map_cls).unwrap();

        let bool_cls = env.find_class("java/lang/Boolean").unwrap();
        let new_bool = env.get_method_id(bool_cls, "<init>", "(Z)V").unwrap();
        let get_bool_value = env.get_method_id(bool_cls, "booleanValue", "()Z").unwrap();
        let bool_cls = env.new_global_ref(bool_cls).unwrap();

        let short_cls = env.find_class("java/lang/Short").unwrap();
        let new_short = env.get_method_id(short_cls, "<init>", "(S)V").unwrap();
        let get_short_value = env.get_method_id(short_cls, "shortValue", "()S").unwrap();
        let short_cls = env.new_global_ref(short_cls).unwrap();

        let int_cls = env.find_class("java/lang/Integer").unwrap();
        let new_int = env.get_method_id(int_cls, "<init>", "(I)V").unwrap();
        let get_int_value = env.get_method_id(int_cls, "intValue", "()I").unwrap();
        let int_cls = env.new_global_ref(int_cls).unwrap();

        let long_cls = env.find_class("java/lang/Long").unwrap();
        let new_long = env.get_method_id(long_cls, "<init>", "(J)V").unwrap();
        let get_long_value = env.get_method_id(long_cls, "longValue", "()J").unwrap();
        let long_cls = env.new_global_ref(long_cls).unwrap();

        let float_cls = env.find_class("java/lang/Float").unwrap();
        let new_float = env.get_method_id(float_cls, "<init>", "(F)V").unwrap();
        let get_float_value = env.get_method_id(float_cls, "floatValue", "()F").unwrap();
        let float_cls = env.new_global_ref(float_cls).unwrap();

        let double_cls = env.find_class("java/lang/Double").unwrap();
        let new_double = env.get_method_id(double_cls, "<init>", "(D)V").unwrap();
        let get_double_value = env.get_method_id(double_cls, "doubleValue", "()D").unwrap();
        let double_cls = env.new_global_ref(double_cls).unwrap();

        let string_cls = env.find_class("java/lang/String").unwrap();
        let string_cls = env.new_global_ref(string_cls).unwrap();

        let instant_cls = env.find_class("java/time/Instant").unwrap();
        let new_instant = env
            .get_static_method_id(instant_cls, "ofEpochSecond", "(JJ)Ljava/time/Instant;")
            .unwrap();
        let get_epoch_second = env
            .get_method_id(instant_cls, "getEpochSecond", "()J")
            .unwrap();
        let get_nano = env.get_method_id(instant_cls, "getNano", "()I").unwrap();
        let instant_cls = env.new_global_ref(instant_cls).unwrap();

        let list_cls = env.find_class("java/util/List").unwrap();
        let list_cls = env.new_global_ref(list_cls).unwrap();

        let map_cls = env.find_class("java/util/Map").unwrap();
        let map_cls = env.new_global_ref(map_cls).unwrap();

        let mut max_arity = 0;
        loop {
            if env.find_class(&format!("com/linkedin/feathr/online/Function{max_arity}")).is_err()
            {
                // NOTE: Here we already triggered a `ClassNotFound` exception, need to clear it before continue.
                env.exception_clear().unwrap();
                break;
            }
            max_arity += 1;
        }

        JavaStates {
            jvm,
            obj_cls,
            to_string,
            illegal_argument_exception_cls,
            runtime_exception_cls,
            array_list_cls,
            new_array_list,
            hash_map_cls,
            new_hash_map,
            bool_cls,
            new_bool,
            get_bool_value,
            short_cls,
            new_short,
            get_short_value,
            int_cls,
            new_int,
            get_int_value,
            long_cls,
            new_long,
            get_long_value,
            float_cls,
            new_float,
            get_float_value,
            double_cls,
            new_double,
            get_double_value,
            string_cls,
            instant_cls,
            new_instant,
            get_epoch_second,
            get_nano,
            list_cls,
            map_cls,
            max_arity,
        }
    });
}

pub fn get_jvm() -> &'static JavaStates {
    // The global JVM cache should already be initialized at the time this function is called.
    JVM.get().unwrap()
}
