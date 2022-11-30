package com.azure.feathr.piper;

import java.util.HashMap;

public class UdfRepository {
    HashMap<String, UserDefinedFunction> udfMap;

    UdfRepository() {
        udfMap = new HashMap<>();
    }

    UdfRepository put(String name, UserDefinedFunction function) {
        udfMap.put(name, function);
        return this;
    }
}
