import feathrpiper
import asyncio

# Lookup Source definition
FAKE_DATA = {
    "1": {"name": "John", "age": 30},
    "2": {"name": "Jane", "age": 58},
    "3": {"name": "Jack", "age": 19},
    "4": {"name": "Jill", "age": 22},
}

async def lookup_source(key, fields):
    """
    The lookup function must be async
    """
    # Pretend to do some slow lookup
    await asyncio.sleep(0.5)
    # Returned value must be a list or list, each list must be align with `fields``
    return [[FAKE_DATA[str(key)][f] for f in fields]]

pipelines = r'''
t(x)
| lookup name, age from fake_src on x
;
'''

async def piper_test():
    """
    As we're using async function, we need to run the pipeline in an async context
    """
    print("Testing Async Lookup...")
    # Piper for local execution, the 2nd argument is the lookup source map
    p = feathrpiper.Piper(pipelines, {"fake_src": lookup_source})

    # In async context, we need to call `process_async` instead of `process`, and we also need to `await` the result.
    (ret, errors) = await p.process_async("t", {"x": 1})
    assert (errors == [])
    assert (len(ret) == 1)
    assert (ret[0]["x"] == 1)
    assert (ret[0]["name"] == "John")
    assert (ret[0]["age"] == 30)

    (ret, errors) = await p.process_async("t", {"x": 4})
    assert (errors == [])
    assert (len(ret) == 1)
    assert (ret[0]["x"] == 4)
    assert (ret[0]["name"] == "Jill")
    assert (ret[0]["age"] == 22)

    print("Tests passed.")


async def piper_service_test():
    """
    Use PiperService to start the service
    Same as the `Piper` test, we need to use `start_async` instead of `start`
    NOTE: This may **not** work on hosted notebook, because the service will be started on the notebook server, which is not accessible from the outside.
    """
    print("Starting service at localhost:8000, press Ctrl+C to stop")
    svc = feathrpiper.PiperService(pipelines, {"fake_src": lookup_source})
    await svc.start_async("localhost", 8000)


async def test():
    await piper_test()
    await piper_service_test()

# Start the async event loop
# Without this, the async function will never be executed
asyncio.run(test())
