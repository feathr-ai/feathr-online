README
=====

This project include 4 components:

* The transformation core, it's a shared component used by all the other components.
* The standalone executable, which is a HTTP server that can be used to transform data, it doesn't support UDF, and the docker image is published to DockerHub as `windoze/feathrpiper:latest`.
* The Python package, it supports UDF written in Python, the package is published to PyPI as `feathrpiper` and can be installed with `pip`.
* The Java package, it supports UDF written in Java, the package is published to GitHub Package Registry as `com.github.windoze.feathr:feathrpiper`.


To start the standalone executable
----------------------------------

Start the service with the command:
```bash
piper -p <PIPELINE_DEFINITION_FILE_NAME> -l <LOOKUP_SOURCE_JSON_FILE_NAME> [--address <LISTENING_ADDRESS>] [--port <LISTENING_PORT>]
```

To start the docker container
-----------------------------

Run the following command:

```bash
docker run -p 8000:8000 windoze/feathrpiper:latest
```

The service will listen on port 8000, and you can send HTTP request to it to transform data, it uses the pre-packaged config located under the `conf` directory.
To use your own config, you can mount a volume to the container, for example:

```bash
mkdir conf

cat > conf/pipeline.conf <<EOF
t(x)
| project y=x+42, z=x-42
;
EOF

cat > conf/lookup.json <<EOF
{}
EOF

docker run -p 8000:8000 -v $(pwd)/conf:/conf windoze/feathrpiper:latest
```

Then you can try out the service with the following command:

```bash
curl -s -XPOST -H"content-type:application/json" localhost:8000/process -d'{"requests": [{"pipeline": "t","data": {"x": 57}}]}'
```

The response will be like:

```json
{
  "results": [
    {
      "status": "OK",
      "count": 1,
      "pipeline": "t",
      "data": {
        "x": 57,
        "y": 99,
        "z": 15
      },
      "time": 0.5
    }
  ]
}
```

The HTTP API spec
-----------------
The request is a POST request to `host:port/process`, and the body is a JSON object with the following fields (some comments added for clarity but they should not be included in the actual request):
```json
{
    // We support multiple requests in one round trip, so this is an array of requests.
    "requests" : [
        // The 1st request
        {
            // The name of the pipeline to use, it should be defined in the pipeline definition file.
            "pipeline": "the_name_of_the_pipeline_to_be_used",
            "data":{
                // These are the values of the input schema defined in the DSL
                "column1": "value1",
                "column2": "value2",
                "column3": "value3",
                ...
            }
        },
        // The 2nd request
        {
            "pipeline": "the_name_of_another_pipeline_to_be_used",
            "data":{
                "column1": "value1",
                "column2": "value2",
                "column3": "value3",
                ...
            }
        },
        ...
    ]
}
```

The response will be in following format:
```json
{
    // Each result is corresponding to the request in the same position.
    "results": [
        {
            // Could be "OK" or "ERROR"
            // "ERROR" means critical error happened and the result could not be generated at all, there won't be `count` and `data` fields in this case.
            "status": "OK",
            // The number of rows in the result.
            "count": 1,
            // The result could contain multiple rows, so this is an array.
            "data": [
                // The 1st row
                {
                    "column1": "value1",
                    "column2": "value2",
                    "column3": "value3",
                    ...
                },
                // The other rows, if there is any
                {
                    "column1": "value1",
                    "column2": "value2",
                    "column3": "value3",
                    ...
                }
            ],
            "pipeline": "the_name_of_the_pipeline_in_the_request",
            // If there is any error, this field will be present.
            "errors": [
                {
                    // `row` and `column` are the position of the error in the `data` field
                    "row": 3,
                    "column": "column2",
                    "message": "Some error message"
                }
            ]
            // The local process time (ms) of this request, it doesn't include the network transfer time.
            "time": 0.35
        },
        {
            // The 2nd result
            // ...
        }
    ]
}
```

DSL
---

The DSL syntax in EBNF format is in the [`DSL-syntax.txt`](DSL-syntax.txt) file.

All the keywords are case sensitive and must be in lowered case.

The list of built-in functions can be found in the [`piper/src/pipeline/function/mod.rs`](piper/src/pipeline/function/mod.rs) file.

TODO:
------

* Error tracing, for now only a string representation of the error is recorded, need to record full stack trace under the debug mode.
* Aggregation, group by, count, avg, etc.
* Hosted data, Parquet, CSV, Delta Lake, etc.?
* Join?
* UDF in WASM?

