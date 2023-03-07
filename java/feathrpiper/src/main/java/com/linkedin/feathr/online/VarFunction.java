package com.linkedin.feathr.online;

import java.util.List;

/**
 * @see UserDefinedFunction
 */
public interface VarFunction extends UserDefinedFunction {
    /**
     *
     * @param arguments is a list of value type described in UserDefinedFunction.
     * @return same as others.
     */
    Object applyVar(List<Object> arguments);
}
