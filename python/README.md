# Feathr Online Transformation Python Support

[![PyPI version](https://badge.fury.io/py/feathrpiper.svg)](https://badge.fury.io/py/feathrpiper) 

This is the Python wrapper of the Feathr online transformation service.

There are 2 major classes in this package:

* `PiperService`, this is the service class, it is used to start a HTTP service to handle the transformation requests. It doesn't support HTTPS and authentication, so you may need to setup gateway or proxy to handle the security issues.
The `PiperService` class has a `start` method to start the HTTP service in blocking mode, and `start_async` method to start the service in the `async` context.

* `Piper`, this is the transformation engine, it can be use to transform data directly, mainly for development and testing purpose.
The `Piper` class has a `process` method to transform data in blocking mode, and `process_async` method to transform data in the `async` context.

Both above classes support [UDF](#user-defined-function-udf-in-python) and [UDLF](#user-defined-lookup-function-udlf-in-python) written in Python.

NOTE: Because of the GIL, pure Python code cannot run concurrently, that means using Python UDF could slow down the transformation service, especially on heavy load.

## Value Types

All values passed to the pipeline must be in one of the following types:
* `None`
* Simple types: `bool`, `int`, `float`, `str`
* Date/time is represented as `datetime.DateTime`.
* List: List of supported types.
* Map: Map of supported types, keys must be string, and value can be any supported type.

All values returned by the pipeline will also be in above types.

NOTE: When using Python big integer, exception will be thrown if any it exceeds the range of 64-bit signed integer.

## User Defined Function (UDF) in Python

The UDF is implemented as ordinary Python function, and it must be registered to the service before it can be used in the pipeline.

* The UDF function can only accept positional arguments, keyword arguments are not supported.
* The UDF function must be able to be invoked by the usage in the DSL script, i.e. a UDF with 2 fixed arguments and 1 optional argument can be invoked as `udf(1, 2)` or `udf(1, 2, 3)`, but not `udf(1, 2, 3, 4)` or `udf(1)`.
* The UDF function may raise any exception, and the returned value will be record as an error. This error will be propagated to the caller.
* Every function and operator that takes error as the input will return the error.
* At the final output stage of the pipeline, the error value will be converted to `None`, and the error will be recorded in a separated error list.
* The UDF function will never see the error as the input, the invocation is bypassed before the UDF function is called if any of the argument is error.
* The execution order is non-deterministic, so the UDF function shall not make any assumptions.
* The UDF function should not block, such behavior is not strictly forbidden but the performance will be impacted significantly.

## User Defined Lookup Function (UDLF) in Python

Usually `lookup` is to fetch external data, such as a database or a web service, so the lookup data source is implemented as a Python async functions, and it must be registered to the piper or the service before it can be used in the pipeline.

The lookup function is called with a single key and a list of requested field names, and it should return a list of rows that each row is a list that aligns with the requested fields, or an empty list when lookup failed. The key must be in the supported simple types, list and dict cannot be used as key, and using `None` as the key will get `None` as the value of all returned fields without actually calling the lookup function.

```
async def my_fancy_lookup_function(key: Any, fields: List[str]) -> List[List[Any]]:
    ...
    return [
        [some_data[f] for f in fields],
        [some_other_data[f] for f in fields],
    ]
```

The lookup function must be added to the `Piper` or `PiperService` before it can be used in the pipeline:
```
piper = Piper(pipeline_def, {"lookup_name": my_fancy_lookup_function}, ...)
```
or
```
svc = PiperService(pipeline_def, {"lookup_name": my_fancy_lookup_function}, ...)
```

Then you can use the lookup data source in the pipeline in a `lookup` transformation:
```
pipeline_name(...)
| ...
| lookup field1, field2 from lookup_name on key
| ...
;
```

or a `join` transformation:
```
pipeline_name(...)
| ...
| join kind=left-inner field1, field2 from lookup_name on key
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
* In order to lookup data from a standard JSON-based HTTP API, you can use builtin HTTP client instead of implementing your own lookup function, register the lookup data source either in a JSON string or a `dict` with correct content, detailed doc is at [here](https://github.com/feathr-ai/feathr-online#lookup-data-source-definition).
* The `feathrpiper` also has builtin support of SqlServer/AzureSQL, Sqlite3, and Azure CosmosDb.

## Integration with Other Web-Service Frameworks

The `feathrpiper` contains built-in web service, but it doesn't support HTTPS and authentication, and has a specific HTTP API spec which cannot be changed from the Python side. In case you need to use it in any other scenario, you may integrate it with other Web service frameworks.

* Flask: prefer to use async version of Flask, such as [Flask-Async](https://pypi.org/project/Flask-Async/), [Flask-RESTful-Async](https://pypi.org/project/Flask-RESTful-Async/), [Flask-RESTX-Async](https://pypi.org/project/Flask-RESTX-Async/), etc. And you should use `process_async` to process the request.
* FastAPI: FastAPI is fully async-based, use `process_async` to process the request.
* Any other Web framework that doesn't support async: You can use `process` in non-async context, but the user-defined lookup function feature will be unavailable.

A demo of integrating with FastAPI is at [here](https://github.com/feathr-ai/feathr-online/blob/main/python/examples/fastapi_example.py)

## Packaging and Deployment

The `feathrpiper` package is a standard Python package without external dependency, you need to write your own code using the package to implement your own transformation service.

The packaging and the deployment process is also standard, refer to [the official document](https://docs.docker.com/language/python/build-images/) if you need to build Docker image, currently we don't have any pre-built Docker image for the Python package.

In most cases, the packaging process could be like:

1. Prepare the `requirements.txt` file which includes the `feathrpiper` package and all the other dependencies.
    ```
    # This package
    feathrpiper >= 0.4.3
    # Any other dependencies
    pandas == 1.5.2
    pytorch >= 1.0.0
    ...
    ```
2. Prepare a `Dockerfile` file which includes the `requirements.txt` file and the code to run the service.
    ```
    FROM python:3.9-slim-buster
    COPY requirements.txt /tmp/
    RUN pip install -r /tmp/requirements.txt
    COPY . /app
    WORKDIR /app
    # In case you want to use the built-in web service provided by `PiperService` class and it's listening at the port 8000
    # Or you write your own web service and it's listening at the port 8000
    EXPOSE 8000
    CMD ["python", "main.py"]
    ```
3. Build the Docker image:
    ```
    docker build -t my_image .
    ```
4. Run the Docker image:
    ```
    docker run -p 8000:8000 my_image
    ```

## Building from Source

The `feathrpiper` package is written in Rust, so you need to setup the Rust toolchain to build it from source. The Rust toolchain can be installed from [here](https://www.rust-lang.org/tools/install). The development is done in Rust 1.65, older version may not work.

1. Install `maturin`:
    ```
    pip install maturin
    ```
2. Build the package under the `feathrpiper_root/python` directory:
    ```
    maturin build --release
    ```
More information about `maturin` can be found [here](https://github.com/PyO3/maturin). Please note that running `cargo build` in the top level directory won't build the Python package because the python package project is excluded from the workspace for some technical issues.

## Limitations and Known Issues

* The `PiperService` class supports plain HTTP only, and it doesn't support any kind of authentication.
* The `feathrpiper` supports Python 3.7~3.11, no support for Python 3.6 or earlier, and no support for Python 2.
* The package published on PyPI only support following platforms:
    * Linux arm64
    * Linux armv7
    * Linux x86_64
    * macOS x86_64/AppleSilicon universal
    * Windows x86_64
  
You need to build the package from source if you need to use it on other platforms.

