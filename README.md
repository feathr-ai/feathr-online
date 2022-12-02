README
=====

This project include 3 components:

* The standalone executable, which is a HTTP server that can be used to transform data, it doesn't support UDF, and the docker image is published to DockerHub as `windoze:feathrpiper:latest`.
* The Python package, it supports UDF written in Python, the package is published to PyPI as `feathrpiper` and can be installed with `pip`.
* The Java package, it supports UDF written in Java, the package is published to GitHub Package Registry as `com.github.windoze.feathr:feathrpiper`.

The standalone executable
-------------------------

Start the service with the command:
```
piper -p <PIPELINE_DEFINITION_FILE_NAME> -l <LOOKUP_SOURCE_JSON_FILE_NAME> [--address <LISTENING_ADDRESS>] [--port <LISTENING_PORT>]
```

TODO:
------

* Error tracing, for now only a string representation of the error is recorded, need to record full stack trace under the debug mode.
* Aggregation, group by, count, avg, etc.
* Hosted data, Parquet, CSV, Delta Lake, etc.?
* Join?
* UDF in WASM?

