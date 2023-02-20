# Monitoring the Online Transformation Service

The Online Transformation Service is packaged into a standard Docker image so it can be deployed on any Docker host. The Docker image is available on Docker Hub and can be pulled with the following command:
```bash
docker pull feathrfeaturestore/feathrpiper:latest
```

To monitor the service, you need to set up logging collection and metrics collection. The following sections describe how to set up logging and metrics collection for the Online Transformation Service.

## The Online Transformation Service Logs

Be default the online transformation service outputs logs in plain text format, which is easy to read by humans, but not easy to be parsed pragmatically. To enable JSON logging, add an `-j` parameter when starting the service. For example:
```bash
docker run -p 8000:8000 -v $(pwd)/conf:/conf feathrfeaturestore/feathrpiper:latest /app/piper -p /conf/pipelines.conf -l /conf/lookups.json -j
```

The output log will look like:
```
{"timestamp":"2023-02-20T18:16:01.280254Z","level":"INFO","fields":{"message":"Piper is starting..."},"target":"piper"}
{"timestamp":"2023-02-20T18:16:01.282476Z","level":"INFO","fields":{"message":"Piper started, listening on 0.0.0.0:8000"},"target":"piper::service"}
{"timestamp":"2023-02-20T18:16:01.282547Z","level":"INFO","fields":{"message":"listening","addr":"socket://0.0.0.0:8000"},"target":"poem::server"}
{"timestamp":"2023-02-20T18:16:01.282562Z","level":"INFO","fields":{"message":"server started"},"target":"poem::server"}
{"timestamp":"2023-02-20T18:17:02.185547Z","level":"INFO","fields":{"pipeline":"nyc_taxi_demo_3_local_compute","time":0.056},"target":"piper::piper","span":{"pipeline":"nyc_taxi_demo_3_local_compute","name":"process"},"spans":[{"method":"POST","remote_addr":"172.17.0.1","uri":"/process","version":"HTTP/1.1","name":"request"},{"pipeline":"nyc_taxi_demo_2_lookup_address","name":"process"},{"pipeline":"nyc_taxi_demo_3_local_compute","name":"process"}]}
{"timestamp":"2023-02-20T18:17:02.926779Z","level":"INFO","fields":{"pipeline":"nyc_taxi_demo_2_lookup_address","time":764.084},"target":"piper::piper","span":{"method":"POST","remote_addr":"172.17.0.1","uri":"/process","version":"HTTP/1.1","name":"request"},"spans":[{"method":"POST","remote_addr":"172.17.0.1","uri":"/process","version":"HTTP/1.1","name":"request"}]}
{"timestamp":"2023-02-20T18:17:02.929540Z","level":"INFO","fields":{"message":"response","status":"200 OK","duration":"767.134917ms"},"target":"poem::middleware::tracing_mw","span":{"method":"POST","remote_addr":"172.17.0.1","uri":"/process","version":"HTTP/1.1","name":"request"},"spans":[{"method":"POST","remote_addr":"172.17.0.1","uri":"/process","version":"HTTP/1.1","name":"request"}]}
```

There is a `target` field in each log, we need to focus on the following targets:
- `poem::middleware::tracing_mw`: the HTTP request logs, each HTTP requests log has a `field` field, which contains the HTTP status code and message, as well as the duration of the request; the `span` field contains the HTTP request details, such as the HTTP method, URI, and remote address.
- `piper::piper`: the pipeline logs, each pipeline log has a `fields` field, which contains the name of the pipeline and the time spent on the pipeline; the `span` field contains the HTTP request details, such as the HTTP method, URI, and remote address.

### Log Collection on Kubernetes

If you are running the Online Transformation Service on Kubernetes, you can use the [Fluentd](https://www.fluentd.org/) to collect the logs. The following is an example of Fluentd configuration snippet:
```xml
<filter kubernetes.**>
  @type parser
  key_name log
  <parse>
    @type json
    json_parser json
  </parse>
  reserve_data true
  remove_key_name_field true
  reserve_time
  emit_invalid_record_to_error false
</filter>
```
Above configuration snippet enables Fluentd to parse the JSON logs and add the `kubernetes` metadata to the logs so they can be further processed by ElasticSearch and Kibana.

### Log Collection on AKS

AKS enables Log Analytics to collect the logs by default. Log Analytics is powered by Kusto, which is a powerful query language for log analytics. The following is an example of Kusto query to get the pipeline request logs:
```kusto
ContainerLog
| where ContainerID in ('<CONTAINER_ID_LIST_OF_ONLINE_TRANSFORMATION_SERVICE>')
| where extractjson("$.target", LogEntry) == 'piper::piper'
| project TimeOfCommand, ContainerID, Piperline = extractjson('$.fields.pipeline', LogEntry), Duration = double(extractjson('$.fields.time', LogEntry))
;
```

Above query outputs 4 columns: `TimeOfCommand`, `ContainerID`, `Piperline`, and `Duration`. The `TimeOfCommand` is the time when the log is generated, `ContainerID` is the ID of the container that generates the log, `Piperline` is the name of the pipeline, and `Duration` is the time spent on the pipeline.

You can write queries to further process the output, such as statistics, visualization, and alerting.

For more information about Log Analytics and Kusto, please refer to the following links:
- [Log Analytics](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/log-analytics-overview)

## The Online Transformation Service Metrics

The service metrics can be fount at `/metrics` endpoint. For example, if the service is running on `localhost:8000`, the metrics can be found at `http://localhost:8000/metrics`.

Currently the integration with metrics collection system, e.g. Prometheus, is on the plan but not fully ready, will be updated later.