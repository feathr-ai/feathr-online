
# Install the `feathrpiper` package
#! pip install -U feathrpiper

import asyncio
import json
import feathrpiper


# Test data:
# PreviousData.Vehicle[0].DashCam has been changed to "Yes" otherwise the percentage cannot be calculated
# PreviousData.Postcode has been changed to "NW1 6XE" otherwise the distance is 0
s = r'''
{
    "ModelInput": {
        "MetaData": {
            "Product": "XYZ",
            "ModelID": "XYZ-V1",
            "ModelRequestDateTime": "2022-12-01T10:10:25+00:00"
        },
        "CurrentData": {
            "Postcode": "BH1 2AT",
            "Vehicle": [
                {
                    "VehicleNumber": "189",
                    "ParkingSensors": "Yes",
                    "AEB": "Yes",
                    "DashCam": "No"
                },
                {
                    "VehicleNumber": "190",
                    "ParkingSensors": "Yes",
                    "AEB": "Yes",
                    "DashCam": "Yes"
                }
            ],
            "Driver": [
                {"DriverNumber": "191", "VehicleNumber": "189"},
                {"DriverNumber": "191", "VehicleNumber": "190"}
            ]
        },
        "PreviousData": {
            "Postcode": "NW1 6XE",
            "Vehicle": [
                {
                    "VehicleNumber": "189",
                    "ParkingSensors": "Yes",
                    "AEB": "Yes",
                    "DashCam": "Yes"
                }
            ],
            "Driver": [{"DriverNumber": "191", "VehicleNumber": "189"}]
        }
    }
}
'''

POSTCODE_LOCATIONS = {
    "BH1 2AT": {
        "latitude": 50.724,
        "longitude": -1.9065
    },
    "NW1 6XE": {
        "latitude": 51.5237,
        "longitude": -0.1585
    }
}

async def lookup_loc(postcode, fields):
    """
    User-Defined Lookup Function for Postcode to geolocation lookup
    In real-world case, this could be a call to a 3rd party API, or a database lookup
    """
    print("Lookup postcode: ", postcode)
    return [POSTCODE_LOCATIONS[str(postcode)][f] for f in fields]


def demo_agg(s):
    """
    UDF implementation of JSON parsing and aggregation
    """
    j = json.loads(s)
    current_all_vehicles = j["ModelInput"]["CurrentData"]["Vehicle"]
    current_has_dash_cam = [
        v for v in current_all_vehicles if v["DashCam"] == "Yes"]
    current_driver_ids = [d["DriverNumber"]
                          for d in j["ModelInput"]["CurrentData"]["Driver"]]
    previous_all_vehicles = j["ModelInput"]["PreviousData"]["Vehicle"]
    previous_has_dash_cam = [
        v for v in previous_all_vehicles if v["DashCam"] == "Yes"]
    previous_driver_ids = [d["DriverNumber"]
                           for d in j["ModelInput"]["PreviousData"]["Driver"]]
    current_dash_cam_percentage = len(
        current_has_dash_cam) / len(current_all_vehicles)
    current_driver_count = len(set(current_driver_ids))
    previous_dash_cam_percentage = len(
        previous_has_dash_cam) / len(previous_all_vehicles)
    previous_driver_count = len(set(previous_driver_ids))
    dash_cam_variation = current_dash_cam_percentage - previous_dash_cam_percentage
    return [
        current_dash_cam_percentage,
        current_driver_count,
        previous_dash_cam_percentage,
        previous_driver_count,
        dash_cam_variation,
    ]


pipelines = r'''
# DSL implementation
t_dsl(s)
| project
    # Extract required fields from JSON
    current_all_vehicles=get_json_array(s, "$.ModelInput.CurrentData.Vehicle[*]"),
    current_has_dash_cam=get_json_array(s, "$.ModelInput.CurrentData.Vehicle[?(@.DashCam=='Yes')]"),
    current_driver_ids=get_json_array(s, "$.ModelInput.CurrentData.Driver[*].DriverNumber"),
    previous_all_vehicles=get_json_array(s, "$.ModelInput.PreviousData.Vehicle[*]"),
    previous_has_dash_cam=get_json_array(s, "$.ModelInput.PreviousData.Vehicle[?(@.DashCam=='Yes')]"),
    previous_driver_ids=get_json_array(s, "$.ModelInput.PreviousData.Driver[*].DriverNumber")
| project
    # len returns int, we need double for the division, otherwise the result will be 0.
    # Casting one of the operands to double is enough, the other one will be elevated automatically.
    current_dash_cam_percentage=double(len(current_has_dash_cam))/len(current_all_vehicles),
    current_driver_count=len(array_distinct(current_driver_ids)),
    previous_dash_cam_percentage=double(len(previous_has_dash_cam))/len(previous_all_vehicles),
    previous_driver_count=len(array_distinct(previous_driver_ids))
| project dash_cam_variation=current_dash_cam_percentage-previous_dash_cam_percentage
| project
    current_postcode = string(get_json_object(s, "$.ModelInput.CurrentData.Postcode")),
    previous_postcode = string(get_json_object(s, "$.ModelInput.PreviousData.Postcode"))
# Lookup current location
| lookup current_lat=latitude, current_long=longitude from postcode_locs on current_postcode
# Lookup previous location
| lookup previous_lat=latitude, previous_long=longitude from postcode_locs on previous_postcode
# Calculate distance between current and previous locations
| project dist = distance(current_lat, current_long, previous_lat, previous_long)
| project-keep
    current_lat, current_long, previous_lat, previous_long,
    current_dash_cam_percentage,
    current_driver_count,
    previous_dash_cam_percentage,
    previous_driver_count,
    dash_cam_variation,
    dist
;

# UDF implementation
t_udf(s)
# Call UDF to do the extraction and aggregation
| project ret=demo_agg(s)
| project
    current_dash_cam_percentage=ret[0],
    current_driver_count=ret[1],
    previous_dash_cam_percentage=ret[2],
    previous_driver_count=ret[3],
    dash_cam_variation=ret[4]
| project
    current_postcode = string(get_json_object(s, "$.ModelInput.CurrentData.Postcode")),
    previous_postcode = string(get_json_object(s, "$.ModelInput.PreviousData.Postcode"))
| lookup current_lat=latitude, current_long=longitude from postcode_locs on current_postcode
| lookup previous_lat=latitude, previous_long=longitude from postcode_locs on previous_postcode
| project dist = distance(current_lat, current_long, previous_lat, previous_long)
| project-keep
    current_lat, current_long, previous_lat, previous_long,
    current_dash_cam_percentage,
    current_driver_count,
    previous_dash_cam_percentage,
    previous_driver_count,
    dash_cam_variation,
    dist
;
'''

# Piper for local execution
p = feathrpiper.Piper(pipelines, {"postcode_locs": lookup_loc}, {"demo_agg": demo_agg})

# Need to be async as we're using async lookup function
async def test():
    print("Testing DSL implementation...")
    (ret1, errors) = await p.process_async("t_dsl", {"s": s})
    print(json.dumps(ret1[0], sort_keys=True, indent=4))
    # There shouldn't be error
    assert (len(errors) == 0)

    print("Testing UDF implementation...")
    (ret2, errors) = await p.process_async("t_udf", {"s": s})
    print(json.dumps(ret2[0], sort_keys=True, indent=4))
    # There shouldn't be error
    assert (len(errors) == 0)

    # These 2 implementations should do the same thing
    assert (ret1 == ret2)

# Run the test
asyncio.run(test())