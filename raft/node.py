from fastapi import FastAPI, BackgroundTasks
import aiohttp
import json
import requests
import time
import sys
import threading
from pydantic import BaseModel
from datetime import datetime, timedelta
last_heartbeat_time = None

app = FastAPI()


class FollowerRegistration(BaseModel):
    follower_port: int

def read_config(file_path):
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        sys.exit("Configuration file not found.")
    except json.JSONDecodeError:
        sys.exit("Configuration file is invalid.")

def write_config(file_path, data):
    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)

def register_with_leader(leader_port, my_port):
    url = f"http://localhost:{leader_port}/register_follower"
    response = requests.post(url, json={"follower_port": my_port})
    if response.status_code != 200:
        print(f"Error registering with leader: {response.content}")
        
        
def send_heartbeat(follower_port):
    try:
        response = requests.get(f"http://localhost:{follower_port}/heartbeat")
        print(f"Heartbeat acknowledged by follower on port {follower_port}: {response.json()}")
    except requests.RequestException:
        print(f"Failed to send heartbeat to follower on port {follower_port}")

def heartbeat_task():
    while True:
        config = read_config("config.json")
        for follower_port in config["follower_nodes"]:
            send_heartbeat(follower_port)
        time.sleep(2)  # Send heartbeat every 2 seconds

# @app.post("/register_follower")
# def register_follower(follower_data: FollowerRegistration):
#     follower_port = follower_data.follower_port
#     config = read_config("config.json")
#     if follower_port not in config["follower_nodes"]:
#         config["follower_nodes"].append(follower_port)
#         write_config("config.json", config)
#     return {"message": "Follower registered"}

def send_heartbeat(follower_port):
    try:
        response = requests.get(f"http://localhost:{follower_port}/heartbeat")
        print(f"Heartbeat acknowledged by follower on port {follower_port}: {response.json()}")
    except requests.RequestException as e:
        print(f"Failed to send heartbeat to follower on port {follower_port}")
        
def monitor_heartbeat():
    global last_heartbeat_time
    while True:
        time.sleep(1)  # Check every second
        if last_heartbeat_time and datetime.now() - last_heartbeat_time > timedelta(seconds=3):
            print("Leader dead")
            last_heartbeat_time = None  # Reset to avoid repeated messages

@app.post("/register_follower")
async def register_follower(follower_data: FollowerRegistration, background_tasks: BackgroundTasks):
    config = read_config("config.json")
    new_follower_port = follower_data.follower_port

    if new_follower_port not in config["follower_nodes"]:
        config["follower_nodes"].append(new_follower_port)
        write_config("config.json", config)

        # Add a background task to send initial heartbeat to new follower
        background_tasks.add_task(send_heartbeat, new_follower_port)

    return {"message": "Follower registered"}


@app.get("/heartbeat")
def heartbeat():
    global last_heartbeat_time
    last_heartbeat_time = datetime.now()
    return {"message": "Acknowledged"}

if __name__ == "__main__":
    my_port = int(sys.argv[1])
    config = read_config("config.json")

    if my_port == config["leader_port"]:
        # Start as leader
        threading.Thread(target=heartbeat_task, daemon=True).start()
    else:
        # Start as follower
        register_with_leader(config["leader_port"], my_port)
        # Start monitoring heartbeats
        threading.Thread(target=monitor_heartbeat, daemon=True).start()

    # Start the FastAPI server
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=my_port)