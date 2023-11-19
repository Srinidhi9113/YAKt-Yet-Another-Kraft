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


# CRUD API Endpoints
@app.post("/register_broker/")
async def register_broker(broker: BrokerRecord):
    filePath = f"./metadata-{broker.brokerPort}.json"
    data = load_data(filePath)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["RegisterBrokerRecords"]['timestamp'] = timestamp
    serverSetup = broker.dict()
    serverSetup["internal_uuid"] = str(uuid.uuid4())
    serverSetup["brokerStatus"] = "ALIVE"
    serverSetup["epoch"] = 0
    data["RegisterBrokerRecords"]["records"].append(serverSetup)
    save_data(filePath,data)
    return serverSetup

@app.get("/brokers/")
async def get_brokers():
    # Logic to get all brokers
    pass

@app.delete("/broker/{broker_id}")
async def delete_broker(broker_id: int):
    # Logic to delete a broker
    pass

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