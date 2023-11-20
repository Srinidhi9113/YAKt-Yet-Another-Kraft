import os
import json
import random
import requests
import time
import sys
import threading
import uuid
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime, timedelta
from schema import *
from utils import *

app = FastAPI()

# Define the base class for Node
class Node:
    def __init__(self, port):
        self.port = port
        self.last_heartbeat_time = None
        self.timeout = 5
        self.heartbeat_interval = 2
        self.current_term = 0
        self.config = self.read_config("config.json")
        self.create_node_files()

    @classmethod
    def initialize_node(cls, port):
        """
        Initialize a Node based on the configuration.
        
        Args:
            port (int): The port number of the node.

        Returns:
            Node: An instance of either Leader or Follower based on the configuration.
        """
        try:
            config = cls.read_config("config.json")
        except:
            config = {"leader_node": None, "follower_nodes": [] , "is_election": False}
            cls.write_config("config.json", config)

        if port == config["leader_node"]:
            return Leader(port)
        else:
            if config["leader_node"] is None:
                config["leader_node"] = port
                cls.write_config("config.json", config)
                return Leader(port)
            else:
                return Follower(port, config["leader_node"])

    def create_node_files(self):
        """
        Create node-specific files and directories.
        """
        os.makedirs(str(self.port), exist_ok=True)
        metadata_template = self.read_metadata_template()
        self.create_or_update_file(f"{self.port}/metadata.json", metadata_template)
        self.create_or_update_file(f"{self.port}/eventlog.json", [])

    def create_or_update_file(self, file_path, data):
        """
        Create or update a file with the provided data.

        Args:
            file_path (str): The path to the file.
            data: The data to write to the file.
        """
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

    @staticmethod
    def read_config(file_path):
        """
        Read the configuration file.

        Args:
            file_path (str): The path to the configuration file.

        Returns:
            dict: The parsed configuration data.
        """
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            sys.exit("Configuration file not found.")
        except json.JSONDecodeError:
            sys.exit("Configuration file is invalid.")

    def read_metadata_template(self):
        """
        Read the metadata schema template.

        Returns:
            dict: The metadata schema template.
        """
        try:
            with open("metajson_schema.json", "r") as file:
                return json.load(file)
        except FileNotFoundError:
            print("Metadata schema file not found.")
            return {}  # Return an empty dict or handle this case as needed
        except json.JSONDecodeError:
            print("Metadata schema file is invalid.")
            return {}  # Return an empty dict or handle this case as needed

    @staticmethod
    def write_config(file_path, data):
        """
        Write data to the configuration file.

        Args:
            file_path (str): The path to the configuration file.
            data: The data to write to the file.
        """
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

    def update_eventlog(self, action, port):
        """
        Update the event log with an action and port information.

        Args:
            action (str): The action to log.
            port (int): The port number associated with the action.
        """
        eventlog = self.read_file(f"{self.port}/eventlog.json")
        eventlog.append({"timestamp": datetime.now().isoformat(), f"port_{action}": port})
        self.create_or_update_file(f"{self.port}/eventlog.json", eventlog)

    def read_file(self, file_path):
        """
        Read data from a file.

        Args:
            file_path (str): The path to the file.

        Returns:
            list: The parsed data from the file.
        """
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []  # or return a default value appropriate for your application
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
            return []  # or return a default value

# Define the Leader class, inheriting from Node
class Leader(Node):
    def __init__(self, port):
        super().__init__(port)
        threading.Thread(target=self.heartbeat_task, daemon=True).start()

    def send_heartbeat(self, follower_port):
        """
        Send a heartbeat to a follower.

        Args:
            follower_port (int): The port of the follower node.
        """
        metadata = self.read_file(f"{self.port}/metadata.json")
        try:
            response = requests.post(f"http://localhost:{follower_port}/heartbeat", json=metadata)
            print(f"Heartbeat acknowledged by follower on port {follower_port}: {response.json()}")
            self.update_eventlog("sent", follower_port)
        except requests.RequestException:
            print(f"Failed to send heartbeat to follower on port {follower_port}")

    def heartbeat_task(self):
        """
        Periodically send heartbeats to all follower nodes.
        """
        while True:
            threads = []
            for follower_port in self.config["follower_nodes"]:
                thread = threading.Thread(target=self.send_heartbeat, args=(follower_port,))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

            time.sleep(self.heartbeat_interval)

# Define the Follower class, inheriting from Node
class Follower(Node):
    def __init__(self, port, leader_port):
        super().__init__(port)
        self.register_with_leader(leader_port)
        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()

    def register_with_leader(self, leader_port):
        """
        Register as a follower with the leader.

        Args:
            leader_port (int): The port of the leader node.
        """
        url = f"http://localhost:{leader_port}/register_follower"
        response = requests.post(url, json={"follower_port": self.port})
        if response.status_code != 200:
            print(f"Error registering with leader: {response.content}")
            
    def remove_leader(self):
        port = self.port
        config = self.read_config("config.json")
        config["leader_node"] = None
        if port in config["follower_nodes"]:
            config["follower_nodes"].remove(port)
        self.write_config("config.json", config)

    def update_leader(self):
        port = self.port
        config = self.read_config("config.json")
        config["leader_node"] = port
        self.write_config("config.json", config)

    def send_request_to_followers(self):
        threads = []
        for follower_port in self.config["follower_nodes"]:
            thread = threading.Thread(target=self.send_request_to_follower, args=(follower_port,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def send_request_to_follower(self, follower_port):
        url = f"http://localhost:{follower_port}/set_leader"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error registering with leader: {response.content}")

    def monitor_heartbeat(self):
        """
        Monitor the leader's heartbeat and check for leader failure.
        """
        while True:
            time.sleep(1)
            if self.last_heartbeat_time and datetime.now() - self.last_heartbeat_time > timedelta(seconds=self.timeout):
                print("Leader is dead")
                break
        config = self.read_config("config.json")
        config["is_election"] = True
        self.write_config("config.json", config)

        random_shutdown_delay = random.randint(1, 10)  # Generate a random delay between 1 to 10 seconds
        print(f"Becoming leader in {random_shutdown_delay} seconds...")
        time.sleep(random_shutdown_delay)  # Sleep for the random delay
        
        config = self.read_config("config.json")
        if config["is_election"]:
            print("I am the leader now")
            
            print("config before modification", config)
            
            config["is_election"] = False
            config["leader_node"] = self.port
            # i want to remove the leader node port from the follower nodes
            for follower_port in config["follower_nodes"]:
                if int(follower_port) == int(self.port):
                    config["follower_nodes"].remove(int(follower_port))
            self.write_config("config.json", config)
            
            config = self.read_config("config.json")
            print("config after modification", config)
            
            self.write_config("config.json", config)
            Node.initialize_node(self.port)
        else:
            config = self.read_config("config.json")
            print("this if the config follower sees", config)
            print("I am a follower now")
            Node.initialize_node(self.port)
                # send request to all the followers to stop the timer and start a new follower

# FastAPI endpoints
@app.post("/register_follower")
async def register_follower(follower_data: FollowerRegistration, background_tasks: BackgroundTasks):
    """
    Endpoint to register a follower with the leader node.

    Args:
        follower_data (FollowerRegistration): Data for registering a follower.
        background_tasks (BackgroundTasks): Background tasks to send a heartbeat to the new follower.

    Returns:
        dict: A message indicating the successful registration of the follower.
    """
    node.config = node.read_config("config.json")
    new_follower_port = follower_data.follower_port

    if new_follower_port not in node.config["follower_nodes"]:
        node.config["follower_nodes"].append(new_follower_port)
        node.write_config("config.json", node.config)

        background_tasks.add_task(node.send_heartbeat, new_follower_port)

    return {"message": "Follower registered"}

@app.post("/heartbeat")
def heartbeat(metadata: dict):
    """
    Endpoint to handle heartbeats from followers.

    Args:
        metadata (dict): Metadata received in the heartbeat request.

    Returns:
        dict: A message indicating acknowledgment.
    """
    if isinstance(node, Follower):
        with threading.Lock():
            node.last_heartbeat_time = datetime.now()
            node.create_or_update_file(f"{node.port}/metadata.json", metadata)
            node.update_eventlog("received", node.port)
    return {"message": "Acknowledged"}


@app.post("/register_broker/")
async def register_broker(broker: BrokerRecord):
    """
    Endpoint to register a broker.

    Args:
        broker (BrokerRecord): Broker information for registration.

    Returns:
        str: The internal UUID of the registered broker.
    """
    filePath = f"{node.port}/metadata.json"
    data = load_data(filePath)
    foundDict = checkBrokerExists(broker.dict(), data["RegisterBrokerRecords"]["records"])
    if foundDict:
        return foundDict['internal_uuid']
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["RegisterBrokerRecords"]['timestamp'] = timestamp
    serverSetup = broker.dict()
    serverSetup["internal_uuid"] = str(uuid.uuid4())
    serverSetup["brokerStatus"] = "ALIVE"
    serverSetup["epoch"] = 0
    data["RegisterBrokerRecords"]["records"].append(serverSetup)
    save_data(filePath, data)
    return serverSetup["internal_uuid"]

if __name__ == "__main__":
    port = int(sys.argv[1])
    node = Node.initialize_node(port)

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)