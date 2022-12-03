
# Install the `feathrpiper` package, and the `sentence_transformers` package, which is used in the `embed` UDF
#! pip install -U feathrpiper sentence_transformers

import feathrpiper

print("The first time running of this demo may take a while because it needs to download the model.")
# Complex UDF demo
from sentence_transformers import SentenceTransformer
print("Loading the model...")
model = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded.")

# UDFs

def inc(x):
    return x+42

def dec(x):
    return x-42

# This is the word embedding UDF, it takes about 10ms for single sentence, so be aware of the performance impact
def embed(s):
    # NOTE: The result can be directly returned because NumPy array supports auto-conversion to List
    # Otherwise you must do the conversion manually.
    return model.encode(s)

# Pipeline definition
# It defined a pipeline 't' with 2 input fields, 'x' and 's' where 'x' should be a number and 's' should be a string or a list of strings.
# The types are omitted so the pipeline will not enforce their actual types, but UDFs require the correct types, otherwise an error will be returned.
# The output will have 5 columns 'x', 's' these are kept from the input, and 'y', 'z', 'e', they are generated in the pipeline.
pipelines = r'''
t(x, s)
| project y=inc(x), z=dec(x), e=embed(s)
;
'''

# Define the UDF map
# Each UDF must have a unique name so it can be used in the pipeline DSL script
UDF = {"inc": inc, "dec": dec, "embed": embed,
# This will raise the exception "Function with name sqrt already exists"    
#    "sqrt": math.sqrt
}

print("Testing Piper functionalities...")
# Piper for local execution
p = feathrpiper.Piper(pipelines, "", UDF)

# This request should be processed correctly
(ret, errors) = p.process("t", {"x": 1, "s": "Hello World"})
assert(errors == [])
assert(len(ret) == 1)
assert(ret[0]["x"] == 1)
assert(ret[0]["y"] == 43)
assert(ret[0]["z"] == -41)
assert(len(ret[0]["e"]) > 100)    # I don't know the exact embedding result, just know it should be there and pretty long

# This request contains the wrong 'x' value so there will be errors
(ret, errors) = p.process("t", {"x": "foo", "s": "Hello World"})
# These 2 values cannot be calculated because the input field 'x' has the wrong type, the UDF will raise exceptions
assert(ret[0]["y"] is None)       # The value of the error field is None
assert(ret[0]["z"] is None)       # The value of the error field is None
assert(len(ret[0]["e"]) > 100)    # This value is correctly generated because it doesn't depend on the input field 'x'
assert(len(errors) == 2)          # 2 output fields in 1 row cannot be calculated, so there are 2 errors
print("Tests passed.")

# Use PiperService to start the service
# NOTE: This may **not** work on hosted notebook, because the service will be started on the notebook server, which is not accessible from the outside.
print("Starting service at localhost:8000, press Ctrl+C to stop")
svc = feathrpiper.PiperService(pipelines, "", UDF)
svc.start("localhost", 8000)

# Now you can use the service like this:
# curl -X POST -H "Content-Type: application/json" http://localhost:8000/process -d '{
#   "requests":[
#       {
#           "pipeline":"t",
#           "data":{
#               "x": 1,
#               "s": "Hello World"
#           }
#       }
#   ]
# }'
#
# And the result should be something like:
# {
#   "results": [
#     {
#       "count": 1,
#       "data": [
#         {
#           "e": [
#             -0.03447725251317024,
#             0.031023245304822922,
#             ...
#           ],
#           "s": "Hello World",
#           "x": 1,
#           "y": 43,
#           "z": -41
#         }
#       ],
#       "pipeline": "t",
#       "status": "OK",
#       "time": 8.579
#     }
#   ]
# }
