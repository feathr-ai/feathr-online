# README

This project include 4 components:

* The transformation core, it's a shared component used by all the other components.
* The standalone executable, which is a HTTP server that can be used to transform data, it doesn't support UDF, and the docker image is published to DockerHub as `windoze/feathrpiper:latest`.
* The Python package, it supports UDF written in Python, the package is published to PyPI as `feathrpiper` and can be installed with `pip`.
* The Java package, it supports UDF written in Java, the package is published to GitHub Package Registry as `com.github.windoze.feathr:feathrpiper`.


## To start the docker container

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

## The HTTP API spec

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

## DSL


The DSL syntax in EBNF format is in the [`DSL-syntax.txt`](DSL-syntax.txt) file.

All the keywords are case sensitive and must be in lowered case.

The list of built-in functions can be found in the [`piper/src/pipeline/function/mod.rs`](piper/src/pipeline/function/mod.rs) file.

## Transformations


Piper DSL supports a set of transformations that can be used to manipulate the data in the pipeline. The transformations are chained together to form a pipeline, and the data flows through the pipeline from the source to the sink.Each transformation takes a row set as the input, and outputs a new row set that is passed to the next transformation. The transformations are categorized into two groups: data manipulation and data lookup. The data manipulation transformations are used to manipulate the data in the pipeline, and the data lookup transformations are used to lookup data from [lookup data sources](#lookup-data-source).

### Data Manipulation Transformations

The data manipulation transformations are used to manipulate the data in the pipeline. The data manipulation transformations are:

* `where`: filter the data in the pipeline by a condition.
* `take`: take the first N rows from the input row set and discards the rest.
* `project`/`project-remove`/`project-rename`/`project-keep`: project the fields in the row set.
* `top`: sort the input row set by the specified criteria and take the first N rows.
* `ignore-errors`: ignore the rows that contain error value in the input row set.
* `summarize`: group the input row set by the specified criteria, and apply aggregations on each group.
* `explode`: explode, or transpose, the input row set by the specified criteria, distributes array value into multiple rows.
* `distinct`: remove duplicate rows from the input row set.

### Lookup Transformations

* `lookup`: lookup data from a lookup data source, applies exactly 1:1 mapping for each input row, and fills the lookup fields with `null` if the lookup failed.
* `join`: lookup data from a lookup data source, applies 1:N mapping for each input row, 2 kinds of joining are supported:
    * `left-inner`, or inner-join, only the rows that have a match in the lookup data source are kept.
    * `left-outer`, or left-join, all the rows are kept, and the lookup fields are filled with `null` if the lookup failed.

## Lookup Data Source


Lookup Data Source is used to integrate with external data, it can return multiple rows of data for a given key expression.

Lookup data sources can be used in `lookup` and `join` transformations, the former is always 1:1 mapping, it will still return a row with null values in all lookup fields even when the lookup failed, while the latter is 1:N mapping, it may turn single input row into a set of rows.

When lookup data source is used in a `lookup` transformation, it acts like `left-outer` but only the first row of lookup result is used for each key.

There are 5 types of builtin lookup data sources:
* Feathr Online Store
* JSON-based HTTP API
* SqlServer 2008 and up / AzureSQL
* Sqlite3
* Azure Cosmos DB

They can be defined in the lookup source definition file, which is a JSON file in following format:
```json
{
    "sources": [
        {
            // source1
        },
        {
            // source2
        }
        ...
    ]
}
```

* Feathr Online Store
```json
{
    // This field indicates this is a Feathr Online Store
    "class": "FeathrRedisSource",
    // The name of the source
    "name": "feathrci",
    "host": "SOME_REDIS_HOST",
    "port": 6379,
    // The password, can be omitted if there is no password
    "password": "SOME_MAGIC_WORD",
    "ssl": false,
    // See the Feathr documentation for more details
    "table": "FEATHER_TABLE_NAME"
}
```

* JSON-based HTTP API
```json
{
    // This field indicates this is a HTTP API source
    "class": "HttpJsonApiSource",
    // The name of the source
    "name": "geoip",
    // The base URL of the API
    "urlBase": "http://ip-api.com",
    // HTTP method, can be GET or POST
    "method": "POST",
    // The `key` part of the URL, if the key needs to be in the URL, or it can be omitted if the key is in any other place.
    "keyUrlTemplate": "/json/$",
    // If the request needs some extra headers to be set, they can be defined here.
    "additionalHeaders": {
        "header": "value"
    },
    // If the key is set in the query param.
    "keyQueryParam": "queryParamName"
    // If the request needs some extra query params, they can be defined here.
    "additionalQueryParams": {
        "param": "value"
    },
    // Auth, can be omitted if no auth needed
    "auth": {
        // The type of the authentication, can be "basic", "bearer", or "aad"
        "type": "basic",
        // For basic auth, the username
        "username": "username",
        // For basic auth, the password
        "password": "password",
        // For bearer auth
        "token":"token",
        // For aad auth, the client credential are acquired from the environment variables, Azure CLI, or the managed identity.
        "resource":"resource",
    },
    // The template of the request body, can be omitted if the request body is not needed.
    "requestTemplate": {
        // Any JSON payload, will be used as the request body
    },
    // If the request body is used, this value indicates where to put the key in the request body, otherwise can be omitted.
    "keyPath": "json_path_to_place_the_key",
    
    // This map defined all available fields from this source, each field is extracted from the HTTP response body by a JSON path.
    "resultPath": {
        "field1": "json_path_to_get_field1",
        "field2": "json_path_to_get_field1",
        //...
    }
}
```

* SqlServer 2008 and up / AzureSQL
```json
{
    // This field indicates this is a MSSQL source
    "class": "mssql",
    // The name of the source
    "name": "SOME_NAME",
    // ADO.Net format connection string
    "connectionString": "CONNECTION_STRING_IN_ADO_NET_FORMAT",
    // The template SQL to fetch rows by key, the key will be replaced with the value of `@P1`
    "sqlTemplate": "select f1, f2, f3 from some_table where key_field = @P1",
    // All fields returned by the SQL query, the field names and order must be aligned with the SQL query.
    "availableFields": [
        "f1",
        "f2",
        "f3",
    ]
}
```

* Sqlite3
```json
{
    // This field indicates this is a Sqlite3 source
    "class": "sqlite",
    // The name of the source
    "name": "SOME_NAME",
    "dbPath": "PATH_TO_DB_FILE",
    // The template SQL to fetch rows by key, the key will be replaced with the value of `:key`
    "sqlTemplate": "select f1, f2, f3 from some_table where key_field = :key",
    // All fields returned by the SQL query, the field names and order must be aligned with the SQL query.
    "availableFields": [
        "f1",
        "f2",
        "f3",
    ]
}
```

* Azure CosmosDb
```json
{
    // This field indicates this is a CosmosDb source
    "class": "cosmosdb",
    // The name of the source
    "name": "SOME_NAME",
    // The CosmosDb account
    "account": "${COSMOS_ACCOUNT}",
    // The CosmosDb API Key
    "apiKey": "${COSMOS_API_KEY}",
    // The CosmosDb Database
    "database": "${COSMOS_DATABASE}",
    // The CosmosDb collection
    "collection": "table1",
    // Optional, use this field to specify the SQL query to fetch the row by condition, the `@key` will be replaced with the key value.
    "query": "SELECT * FROM table1 c WHERE c.key0 = @key"
}
```

Fields that may contain secrets can use `${ENV_VAR_NAME}` as its value, the value will be replaced with the value of the environment variable `ENV_VAR_NAME` when the lookup source is loaded. In this way, you can make the lookup definition file open while still keep the secrets safe, and you can use different set of environment variables to work with different data sources.


## Building from Source


The `feathrpiper` package is written in Rust, so you need to setup the Rust toolchain to build it from source. The Rust toolchain can be installed from [here](https://www.rust-lang.org/tools/install). The development is done in Rust 1.65, older version may not work.

Run `cargo build --release` to build the binary, the standalone executable will be in `target/release/piper`, and the JNI library will be in `target/release/libfeathr_piper_jni.so`.

## Running the standalone executable


Start the service with the command:
```bash
/path/to/piper -p <PIPELINE_DEFINITION_FILE_NAME> -l <LOOKUP_SOURCE_JSON_FILE_NAME> [--address <LISTENING_ADDRESS>] [--port <LISTENING_PORT>]
```

## TODO:


- [x] Aggregation, group by, count, avg, etc.
- [x] Join
- [ ] Error tracing, for now only a string representation of the error is recorded, need to record full stack trace under the debug mode.
- [ ] Hosted data, Parquet, CSV, Delta Lake, etc.?
- [ ] UDF in WASM?

