from typing import List
from pydantic import BaseModel

from pydantic import BaseModel
from typing import List

class BrokerRecord(BaseModel):
    brokerId: int
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    rackId:str

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