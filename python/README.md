README
======

This is the Python wrapper of the Feathr online transformation service.

There are 2 major classes in this package:

* `PiperService`, this is the service class, it is used to start a HTTP service to handle the transformation requests. It doesn't support HTTPS and authentication, so you may need to setup gateway or proxy to handle the security issues.

* `Piper`, this is the transformation engine, it can be use to transform data directly, mainly for development and testing purpose.

Both above classes support UDF written in Python.

NOTE: Because of the GIL, pure Python code cannot run concurrently, that means using Python UDF could slow down the transformation service, especially on heavy load.

UDF in Python
-------------

The UDF is implemented as a Python function, and it must be registered to the service before it can be used in the pipeline.

* The UDF function can only accept positional arguments, keyword arguments are not supported.
* The UDF function must be able to be invoked by the usage in the DSL script, i.e. a UDF with 2 fixed arguments and 1 optional argument can be invoked as `udf(1, 2)` or `udf(1, 2, 3)`, but not `udf(1, 2, 3, 4)` or `udf(1)`.
* The arguments are always in following types:
    * `None`
    * Simple types: `bool`, `int`, `float`, `str`
    * Date/time is represented as `datetime.DateTime`.
    * List: List of supported types.
    * Map: Map of supported types, keys must be string, and value can be any supported type.
* The return value must be in above types.
* The UDF function may raise any exception, the returned value will be recorded as an error.
* Any operation with error as the input will result in an error as the output.
* The UDF function will never see the error as the input, the invocation is bypassed before the UDF function is called if any of the argument is error.
* The execution order is non-deterministic, so the UDF function shall not make any assumptions.
* The UDF function should not block, such behavior is not strictly forbidden but the performance will be impacted significantly.