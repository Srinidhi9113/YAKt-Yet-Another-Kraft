from typing import List
from pydantic import BaseModel

class RegisterBrokerRecord(BaseModel):
    internalUUID: str
    brokerId: int
    brokerHost: str
    brokerPort: str
    securityProtocol: str
    brokerStatus: str
    rackId: str
    epoch: int

class RegisterBrokerRecords(BaseModel):
    records: List[RegisterBrokerRecord]
    timestamp: str

class TopicRecord(BaseModel):
    records: List[dict]  # You can create a Pydantic model for this dictionary if needed
    timestamp: str

class PartitionRecord(BaseModel):
    partitionId: int
    topicUUID: str
    replicas: List[str]
    ISR: List[str]
    removingReplicas: List[str]
    addingReplicas: List[str]
    leader: str
    partitionEpoch: int

class PartitionRecords(BaseModel):
    records: List[PartitionRecord]
    timestamp: str

class ProducerIdsRecord(BaseModel):
    brokerId: str
    brokerEpoch: int
    producerId: int

class ProducerIdsRecords(BaseModel):
    records: List[ProducerIdsRecord]
    timestamp: str

class RegistrationChangeBrokerRecord(BaseModel):
    brokerId: str
    brokerHost: str
    brokerPort: str
    securityProtocol: str
    brokerStatus: str
    epoch: int

class RegistrationChangeBrokerRecords(BaseModel):
    records: List[RegistrationChangeBrokerRecord]
    timestamp: str