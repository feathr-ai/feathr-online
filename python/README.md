README
======

This is the Python wrapper of the Feathr online transformation service.

There are 2 major classes in this package:

* `PiperService`, this is the service class, it is used to start a HTTP service to handle the transformation requests. It doesn't support HTTPS and authentication, so you may need to setup gateway or proxy to handle the security issues.

* `Piper`, this is the transformation engine, it can be use to transform data directly, mainly for development and testing purpose. User may also use this class to implement their own transformation service as long as the performance is not the concern.

Both above classes support UDF written in Python.

NOTE: Because of the GIL, pure Python code cannot run concurrently, that means using Python UDF could slow down the transformation service, especially on heavy load.