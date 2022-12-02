FeathrPiper Java support
=======================

FeathrPiper is the Java wrapper for the core package, with the Java UDF support.

It provides the `PiperService` and `UdfRepository` classes and set of function interfaces.

* `PiperService` is the service class, it is used to start a HTTP service to handle the transformation requests. It doesn't support HTTPS and authentication, so you may need to setup gateway or proxy to handle the security issues.
    * `public PiperService(String pipelines, String lookups, UdfRepository repo)`
        Construct the service with the given pipeline and lookup definitions, and the UDF repository.
        * `pipelines` is the content of the pipeline scripts.
        * `lookups` is the content of the lookup data source definitions.
        * `repo` is the UDF repository.
    * `public void start(String address, int port)`
        Start the service at the given address/port. This function is blocked until the service is stopped.
        * `address` is the address to bind to.
        * `port` is the port to listen to.
    * `public void stop()`
        Stop the service. Because the `start` function is blocked, this function should be called in another thread.
* `UdfRepository` is the UDF repository, it is used to register the UDF functions.
    * `public UdfRepository put(String name, UserDefinedFunction function)`
        Register a UDF function with the given name so it can be used in the DSL script.
* `Function0`/`Function1`/`Function2`/`Function3`/`VarFunction` are the UDF function interfaces, UDF must implement one of them.
    * `public interface Function0<R>`
        A UDF function with no argument.
        * `Object apply0()` Call the function.
    * `public interface Function1<T1, R>`
        A UDF function with 1 argument.
        * `Object apply1(Object arg1)` Call the function with 1 argument.
    * `public interface Function2<T1, T2, R>`
        A UDF function with 2 arguments.
        * `Object apply2(Object arg1, Object arg2)` Call the function with 2 arguments.
    * `public interface Function3<T1, T2, T3, R>`
        A UDF function with 3 arguments.
        * `Object apply3(Object arg1, Object arg2, Object arg3)` Call the function with 3 arguments.
    * `public interface VarFunction<R>`
        A UDF function with variable number of arguments.
        * `Object applyVar(List<Object> arguments)` Call the function with variadic number of arguments.

UDF in Java
-----------

The UDF is implemented as a Java function, and it must be registered to the service before it can be used in the pipeline.

* UDF function must implement `Function0` - `Function3` interfaces or the `VarFunction` interface, the latter takes variadic arguments as a `List`.
* Since the function interfaces are all SAM, so lambda expression can also be used.
* The arguments are always in following types:
    * `null`
    * Simple types: `Boolean`, `Integer`, `Long`, `Float`, `Double`, `String`
    * Date/time is represented as `Instant`, and the timezone is always UTC.
    * List: List of supported types.
    * Map: Map of supported types, keys must be string, and value can be any supported type.
* The return type must be in above types.
* The number value taken from the HTTP requests are always in `Long` or `Double`, but still you can use `Integer` and `Float` as the intermediate type and the result type.
* Due to the limitation of Java Generic, all arguments are passed in as the `Object`, the actual type of the arguments must be checked by the UDF function, and the UDF function must be able to handle the error case.
* UDF function may throw exceptions, and the returned value will be recorded as an error.
* The service core is using asynchronous programming, so the UDF function must be thread-safe.
* The UDF must not block, such behavior is not strictly forbidden but the performance will be impacted significantly.
    * We have the plan to support asynchronous UDF in the future, but it is not implemented yet.

NOTE: The argument type is always `Object` and must be checked at runtime, this could be very verbose in some cases but we're actively exploring the value types introduced by Java Valhalla project to see if it can improve the Java generic experiences.


Packaging and Deployment
------------------------

The FeathrPiper Java package is provided as a JAR package, you need to build the service, package it, and deploy it by yourself.
If you don't need Java UDF support, you can use the standalone version of the FeathrPiper, which is published as a Docker image.

The FeathrPiper Java package is published to GitHub Package Registry, so you can add the dependency to your project using the following snippet:
```xml
<dependency>
  <groupId>com.github.windoze.feathr</groupId>
  <artifactId>feathrpiper</artifactId>
  <version>0.2.1</version>
</dependency>
```

You also need to config Maven settings to use multiple repositories, check out Maven manual for more details.



## Sample Usage

```java
package piper.example;

import com.github.windoze.feathr.piper.Function1;
import com.github.windoze.feathr.piper.PiperService;
import com.github.windoze.feathr.piper.UdfRepository;

class PiperServiceExample
{
    static Object inc(Object arg) {
        // Assume the argument is always a Long, throw exception if not.
        Long n = (Long) arg;
        return n + 42;
    }

    static Object dec(Object arg) {
        // Assume the argument is always a Long, throw exception if not.
        Long n = (Long) arg;
        return n - 42;
    }

    public static void main(String[] args) {
        // Register 2 UDFs into the repository.
        UdfRepository repo = new UdfRepository()
                .put("inc", (Function1) PiperServiceExample::inc)
                .put("dec", (Function1) PiperServiceExample::dec);

        // Create the service with the pipeline definition and UDFs. The 2nd argument is the lookup data source definition which is not mentioned in this example.
        try {
            PiperService svc = new PiperService("t(x) | project y=inc(x), z=dec(x);", "", repo);
            new Thread(() -> {
                svc.start("localhost", (short) 8000);
            }).start();
            // Let the service run for 60 seconds
            Thread.sleep(60 * 1000);
            // Stop the service
            svc.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```