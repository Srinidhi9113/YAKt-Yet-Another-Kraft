from pydantic import BaseModel
from typing import List

class BrokerRecord(BaseModel):
    brokerId: int
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    rackId:str

class BrokerChangeRecord(BaseModel):
    brokerId: int # ?? Changed it to int <-----
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    brokerStatus:str

class TopicRecord(BaseModel):
    name: str

class PartitionRecord(BaseModel):
    partitionId: int
    replicas: List[str]
    ISR: List[str]
    leader: str


class ProducerIdsRecord(BaseModel):
    brokerId: str
    brokerEpoch: int
    producerId: int
