import os
from fastapi import FastAPI, BackgroundTasks
import json
import random
import requests
import time
import sys
import threading
from pydantic import BaseModel
from datetime import datetime, timedelta

app = FastAPI()

class Node:
    def __init__(self, port):
        self.port = port
        self.last_heartbeat_time = None
        self.timeout = 5
        self.heartbeat_interval = 2
        self.current_term = 0
        self.config = self.read_config("config.json")
        self.create_node_files()

    def create_node_files(self):
        os.makedirs(str(self.port), exist_ok=True)
        metadata_template = self.read_metadata_template()
        self.create_or_update_file(f"{self.port}/metadata.json", metadata_template)
        self.create_or_update_file(f"{self.port}/eventlog.json", [])

    def create_or_update_file(self, file_path, data):
      with open(file_path, 'w') as file:
          json.dump(data, file, indent=4)

    @staticmethod
    def read_config(file_path):
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            sys.exit("Configuration file not found.")
        except json.JSONDecodeError:
            sys.exit("Configuration file is invalid.")

    def read_metadata_template(self):
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
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
            
    def update_eventlog(self, action, port):
        eventlog = self.read_file(f"{self.port}/eventlog.json")
        eventlog.append({"timestamp": datetime.now().isoformat(), f"port_{action}": port})
        self.create_or_update_file(f"{self.port}/eventlog.json", eventlog)

    def read_file(self, file_path):
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []  # or return a default value appropriate for your application
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
            return []  # or return a default value

class Leader(Node):
    def __init__(self, port):
        super().__init__(port)
        threading.Thread(target=self.heartbeat_task, daemon=True).start()

    def send_heartbeat(self, follower_port):
        metadata = self.read_file(f"{self.port}/metadata.json")
        try:
            response = requests.post(f"http://localhost:{follower_port}/heartbeat", json=metadata)
            print(f"Heartbeat acknowledged by follower on port {follower_port}: {response.json()}")
            self.update_eventlog("sent", follower_port)
        except requests.RequestException:
            print(f"Failed to send heartbeat to follower on port {follower_port}")

    def heartbeat_task(self):
        while True:
            threads = []
            for follower_port in self.config["follower_nodes"]:
                thread = threading.Thread(target=self.send_heartbeat, args=(follower_port,))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

            time.sleep(self.heartbeat_interval)

class Follower(Node):
    def __init__(self, port, leader_port):
        super().__init__(port)
        self.register_with_leader(leader_port)
        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()

    def register_with_leader(self, leader_port):
        url = f"http://localhost:{leader_port}/register_follower"
        response = requests.post(url, json={"follower_port": self.port})
        if response.status_code != 200:
            print(f"Error registering with leader: {response.content}")

    def monitor_heartbeat(self):
        while True:
            time.sleep(1)
            if self.last_heartbeat_time and datetime.now() - self.last_heartbeat_time > timedelta(seconds=self.timeout):
                print("Leader is dead")
                self.become_candidate()

    def become_candidate(self):
        # Candidate logic
        pass

class Candidate(Node):
    def __init__(self, port):
        super().__init__(port)
        # Candidate specific initialization

# Define Pydantic models
class FollowerRegistration(BaseModel):
    follower_port: int

# FastAPI endpoints
@app.post("/register_follower")
async def register_follower(follower_data: FollowerRegistration, background_tasks: BackgroundTasks):
    node.config = node.read_config("config.json")
    new_follower_port = follower_data.follower_port

    if new_follower_port not in node.config["follower_nodes"]:
        node.config["follower_nodes"].append(new_follower_port)
        node.write_config("config.json", node.config)

        background_tasks.add_task(node.send_heartbeat, new_follower_port)

    return {"message": "Follower registered"}

@app.post("/heartbeat")
def heartbeat(metadata: dict):
    if isinstance(node, Follower):
        with threading.Lock():
            node.last_heartbeat_time = datetime.now()
            node.create_or_update_file(f"{node.port}/metadata.json", metadata)
            node.update_eventlog("received", node.port)
    return {"message": "Acknowledged"}

if __name__ == "__main__":
    port = int(sys.argv[1])
    try:
        config = Node.read_config("config.json")
    except:
        config = {"leader_node": None, "follower_nodes": []}
        Node.write_config("config.json", config)

    if port == config["leader_node"]:
        node = Leader(port)
    else:
        if config["leader_node"] is None:
            config["leader_node"] = port
            Node.write_config("config.json", config)
            node = Leader(port)
        else:
            node = Follower(port, config["leader_node"])

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)