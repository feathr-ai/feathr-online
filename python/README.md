README
======

This is the Python wrapper of the Feathr online transformation service.

There are 2 major classes in this package:

* `PiperService`, this is the service class, it is used to start a HTTP service to handle the transformation requests.

* `Piper`, this is the transformation engine, it can be use to transform data locally, mainly for development and testing purpose. User may also use this class to implement their own transformation service as long as the performance is not the concern.

Both above classes support UDF written in Python.