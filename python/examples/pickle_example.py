####################################################################################################
# The `Piper` class supports pickling, so it can be used in multiprocessing, or even in PySpark.
# The UDFs must be picklable, so they can be pickled along with the `Piper` instance.
# The `pickle` has some limitations, so if you want to use more complex UDFs, you can use `cloudpickle` instead,
# and the latter is also used by PySpark.
#
# NOTE: There is no support of pickling in class `PiperService`.

import pickle
from feathrpiper import Piper

def inc(x):
    return x+42


def dec(x):
    return x-42

pipelines = r'''
t(x, s)
| project y=inc(x), z=dec(x)
;
'''

UDF = {"inc": inc, "dec": dec}
p = Piper(pipelines, functions=UDF)

# Picking/Unpickling with `pickle`.
s = pickle.dumps(p)
p = pickle.loads(s)
(ret, errors) = p.process("t", {"x": 1, "s": "Hello World"})
assert (errors == [])
assert (len(ret) == 1)
assert (ret[0]["x"] == 1)
assert (ret[0]["y"] == 43)
assert (ret[0]["z"] == -41)


# This also works with `cloudpickle`.
# Commented out because `cloudpickle` is not installed by default.
#
# import cloudpickle
# s = cloudpickle.dumps(p)
# p = cloudpickle.loads(s)
# (ret, errors) = p.process("t", {"x": 1, "s": "Hello World"})
# assert (errors == [])
# assert (len(ret) == 1)
# assert (ret[0]["x"] == 1)
# assert (ret[0]["y"] == 43)
# assert (ret[0]["z"] == -41)
