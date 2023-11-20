from pydantic import BaseModel

class LeaderInformation(BaseModel):
    new_leader_port: int

class FollowerRegistration(BaseModel):
    follower_port: int
    
class VoteRequest(BaseModel):
    candidate_port: int
    # term: int

class LeaderData(BaseModel):
    leader_port: int

class BrokerRecord(BaseModel):
    brokerId: int
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    rackId:str
    