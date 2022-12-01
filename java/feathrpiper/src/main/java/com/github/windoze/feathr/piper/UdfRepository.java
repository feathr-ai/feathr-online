package com.github.windoze.feathr.piper;

import java.util.HashMap;

public class UdfRepository {
    final HashMap<String, UserDefinedFunction> udfMap;

    public UdfRepository() {
        udfMap = new HashMap<>();
    }

    public UdfRepository put(String name, UserDefinedFunction function) {
        udfMap.put(name, function);
        return this;
    }
}
