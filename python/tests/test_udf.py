import feathrpiper

# UDFs

def inc(x):
    return x+42

def dec(x):
    return x-42

# Pipeline definition
pipelines = r'''
t(x)
| project y=inc(x), z=dec(x)
;
'''

# Piper for local execution
p = feathrpiper.Piper(pipelines, "", {"inc": inc, "dec": dec})
(ret, errors) = p.process("t", {"x": 1})
print(ret)
assert(errors == [])
assert(ret == [{"x":1, "y": 43, "z": -41}])

# Start the service
svc = feathrpiper.PiperService(pipelines, "", {"inc": inc, "dec": dec})
svc.start("localhost", 8000)