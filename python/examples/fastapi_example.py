############################################################################################################
# Integration of `feathrpiper` and `FastAPI` example
#
# Install required packages before running this example:
# `pip install feathrpiper fastapi uvicorn`
############################################################################################################

import feathrpiper

from typing import Union

from fastapi import FastAPI, Response, status
from pydantic import BaseModel

############################################################################################################
# FastAPI app object

app = FastAPI()

############################################################################################################
# Some demo UDF


def inc(x):
    return x+42


def dec(x):
    return x-42


############################################################################################################
# The pipeline definition
pipelines = r'''
t(x as int)
| project y=inc(x), z=dec(x)
;
'''

############################################################################################################
# Create the global `piper` instance, the instance is immutable so it's safe to share it across requests.
# We use `Piper` instead of `PiperServer` as we will use FastAPI instead of the built-in server.
piper = feathrpiper.Piper(pipelines, {}, {"inc": inc, "dec": dec})

############################################################################################################
# Route handler


@app.post("/pipelines/{pipeline_name}/process")
async def create_item(pipeline_name: str, req: dict, response: Response) -> list[dict]:
    # Should use `async` version of `process` here for better performance as FastAPI is fully async-aware
    (ret, errors) = await piper.process_async(pipeline_name, req)
    if errors:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return errors
    return ret

############################################################################################################
# Run the service with:
# ```
# uvicorn fastapi_example:app --reload
# ```
#
############################################################################################################
# The service accepts requests like:
# ```json
# {
#     "x": 99
# }
# ```
#
# and returns:
# ```json
# [
#   {
#     "x": 99,
#     "y": 141,
#     "z": 57
#   }
# ]
# ```
############################################################################################################
