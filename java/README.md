FeathrPiper Java support
=======================

FeathrPiper is the Java wrapper for the core package, with the Java UDF support.

## Usage

```java
package piper.example;

import com.github.windoze.feathr.piper.Function1;
import com.github.windoze.feathr.piper.PiperService;
import com.github.windoze.feathr.piper.UdfRepository;

class PiperServiceExample
{
    // Arguments and return value of the UDF are all in `Object` type.
    // You need to cast them to the expected type.
    // Supported types include:
    // * null
    // * Simple types: Boolean, Integer, Long, Float, Double, String
    // * Date/time is represented as java.util.Instant, and the timezone is always UTC.
    // * List: List of supported types.
    // * Map: Map of supported types, keys must be string, and value can be any supported type.
    //
    // NOTE: The service core is using asynchronous IO, so the UDF must not block, otherwise the performance will be impacted significantly.
    
    static Object inc(Object arg) {
        Long n = (Long) arg;
        return n + 42;
    }

    static Object dec(Object arg) {
        Long n = (Long) arg;
        return n - 42;
    }

    public static void main(String[] args) {
        // Register 2 UDFs into the repository.
        UdfRepository repo = new UdfRepository()
                .put("inc", (Function1) PiperServiceExample::inc)
                .put("dec", (Function1) PiperServiceExample::dec);

        // Create the service with the pipeline definition and UDFs.
        // The service implements AutoCloseable so we should wrap it in a try block.
        try (PiperService svc = new PiperService("t(x) | project y=inc(x), z=dec(x);", "", repo)) {
            // Start the service in a new thread
            new Thread(() -> {
                // The service is now listening on localhost:8000
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