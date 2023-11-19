from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Union
import json
import uuid

app = FastAPI()

# Define your Pydantic models
class BrokerRecord(BaseModel):
    internalUUID: Optional[str] = Field(default_factory=uuid.uuid4, alias="internalUUID")
    brokerId: int
    brokerHost: str
    brokerPort: str
    securityProtocol: str
    brokerStatus: str
    rackId: str
    epoch: int

class BrokerOperation(BaseModel):
    operation: str  # "register", "get_all", "get_by_id"
    broker: Optional[BrokerRecord] = None
    brokerId: Optional[int] = None

# Load and save data functions
def load_data():
    try:
        with open("metadata.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {"RegisterBrokerRecords": {"records": [], "timestamp": ""}}

def save_data(data):
    with open("metadata.json", "w") as file:
        json.dump(data, file, indent=4)

# Combined Broker API
@app.post("/broker_operation/")
async def broker_operation(operation: BrokerOperation):
    data = load_data()

    if operation.operation == "register":
        if operation.broker:
            data["RegisterBrokerRecords"]["records"].append(operation.broker.dict(by_alias=True))
            save_data(data)
            return {"internalUUID": operation.broker.internalUUID}
        else:
            raise HTTPException(status_code=400, detail="Broker data is required for registration")

    elif operation.operation == "get_all":
        return data["RegisterBrokerRecords"]["records"]

    elif operation.operation == "get_by_id":
        if operation.brokerId is not None:
            broker = next((b for b in data["RegisterBrokerRecords"]["records"] if b["brokerId"] == operation.brokerId), None)
            if broker:
                return broker
            else:
                raise HTTPException(status_code=404, detail="Broker not found")
        else:
            raise HTTPException(status_code=400, detail="Broker ID is required")

    else:
        raise HTTPException(status_code=400, detail="Invalid operation")

# Main function to run the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)