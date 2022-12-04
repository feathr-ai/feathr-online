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

Lookup Data Source in Python
----------------------------

Usually `lookup` is to fetch external data, such as a database or a web service, so the lookup data source is implemented as a Python async functions, and it must be registered to the piper or the service before it can be used in the pipeline:

The lookup function is called with a single key and a list of requested field names, and it should return a list of values for the requested fields.
```
async def lookup(key: Any, fields: List[str]):
    ...
    return [some_data[f] for f in fields]
```

It must be added to the `Piper` or `PiperService` before it can be used in the pipeline:
```
piper = Piper(pipeline_def, {"lookup_name": lookup_function})
```
or
```
svc = PiperService(pipeline_def, {"lookup_name": lookup_function})
```

Then you can use the lookup data source in the pipeline:
```
pipeline_name(...)
| ...
| lookup field1, field2 from lookup_name on key
| ...
;
```

Once the user-defined lookup function is used, the `Piper` and `PiperService` must be used in `async` context, otherwise all async function will never be executed and the program may hang forever.
Also you need to replace `process` with `process_async`, and `start` with `start_async`.

```
piper = Piper(pipeline_def, {"lookup_name": lookup_function})

async def test():
    await piper.process_async(...)

asyncio.run(test())
```

For more information about Python async programming, please refer to [Python Asyncio](https://docs.python.org/3/library/asyncio.html).

NOTE:
* Because of the asynchronous nature of the lookup function, it's recommended to use `asyncio` compatible libraries to implement the lookup function, traditional blocking libraries may cause the performance issue, e.g. use [`aiohttp`](https://pypi.org/project/aiohttp/) or [`HTTPX`](https://pypi.org/project/httpx/) instead of `Requests`.
* This package only supports `asyncio`,  `Twisted` or `Gevent` based libraries are not supported.
* In order to lookup data from a standard JSON-based HTTP API, you can use builtin HTTP client instead of implementing your own lookup function, register the lookup data source either in a JSON string or a `dict` with correct content, detailed doc is at [here](https://github.com/windoze/piper#lookup-data-source-definition).
