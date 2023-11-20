from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import json
from datetime import datetime
import uuid

app = FastAPI()

# Define your Pydantic models here
class BrokerRecord(BaseModel):
    brokerId: int
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    rackId:str

class TopicRecord(BaseModel):
    # Define the fields
    pass

# ... Define other models

# Utility functions to handle data storage and retrieval
def load_data(path):
    try:
        with open(path, 'r') as file:
            data = json.load(file)
            return data
    except:
        data =  {
            "RegisterBrokerRecords": {
                "records": [],
                "timestamp": ""
            },
            "TopicRecord": {
                "records": [],
                "timestamp": ""
            },
            "PartitionRecord": {
                "records": [],
                "timestamp": ""
            },
            "ProducerIdsRecord": {
                "records": [],
                "timestamp": ""
            },
            "RegistrationChangeBrokerRecord": {
                "records": [],
                "timestamp": ""
            }
        }
        return data

def save_data(path,data):
    with open(path, 'w') as file:
        json.dump(data, file, indent=2)

def checkBrokerExists(brokerData,data):
    found_dict = next((broker for broker in data if broker.get("brokerId") == brokerData['brokerId']), None)
    return found_dict


# CRUD API Endpoints
@app.post("/register_broker/")
async def register_broker(broker: BrokerRecord):
    filePath = "./metadata-8000.json"
    data = load_data(filePath)
    foundDict = checkBrokerExists(broker.dict(),data["RegisterBrokerRecords"]["records"])
    if(foundDict):
        return foundDict['internal_uuid']
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["RegisterBrokerRecords"]['timestamp'] = timestamp
    serverSetup = broker.dict()
    serverSetup["internal_uuid"] = str(uuid.uuid4())
    serverSetup["brokerStatus"] = "ALIVE"
    serverSetup["epoch"] = 0
    data["RegisterBrokerRecords"]["records"].append(serverSetup)
    save_data(filePath,data)
    return serverSetup["internal_uuid"]

@app.get("/brokers/")
async def get_broker():
    filePath = "./metadata-8000.json"
    data = load_data(filePath)
    return data["RegisterBrokerRecords"]["records"]

@app.delete("/broker/{broker_id}")
async def delete_broker(broker_id: int):
    filePath = "./metadata-8000.json"
    data = load_data(filePath)
    brokers = data["RegisterBrokerRecords"]["records"]
    index = next((index for index, broker in enumerate(brokers) if broker.get("brokerId") == broker_id), None)
    if index is not None:
        deletedBroker = brokers.pop(index)
        data["RegisterBrokerRecords"]["records"] = brokers
        save_data(filePath,data)
        return deletedBroker
    return "Broker Does Not Exist"

# ... Define other CRUD endpoints

# Broker Management API Endpoints
@app.post("/register_broker_change/")
async def register_broker_change():
    # Logic for broker change
    pass

@app.post("/metadata_fetch/")
async def metadata_fetch():
    # Logic for metadata fetch
    pass

# ... Define other Broker Management endpoints

# Client Management API Endpoints
@app.post("/register_producer/")
async def register_producer():
    # Logic to register a producer
    pass

@app.get("/metadata_fetch_client/")
async def metadata_fetch_client():
    # Logic for client metadata fetch
    pass

# ... Define other Client Management endpoints

# Main function to run the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)