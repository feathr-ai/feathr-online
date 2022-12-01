package com.github.windoze.feathr.piper;

/**
 * <p>This is a marker interface for all user defined function types.</p>
 * <p>User defined function takes 0 or more Object as the arguments and return one Object,
 * the actual value of the arguments and the returned object must be in the type that meets following requirements.</p>
 * The type requirements include:
 * <ol>
 * <li>A simple type, includes Boolean, Int, Long, Float, Double, String, Instant, or a null value.</li>
 * <li>A List type, nesting is allowed, means it can be a list with list or map as its element.</li>
 * <li>A Map type, the key must be non-null String, and the value can be any value type, nesting is allowed.</li>
 * </ol>
 * The arguments passed in always meet above requirement, and the UDF needs to guarantee the returned value also meets
 * above requirements, otherwise the returned value will be converted into null, and an error will be recorded in the final
 * output.
 */
public interface UserDefinedFunction {
}
