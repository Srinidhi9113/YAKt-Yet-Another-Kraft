## CRD API

### Create

- **Endpoint**: `/create_record/`
- **Method**: POST
- **Description**: Creates a new record in the system. Returns the unique ID of the created entity if available.
- **Request Body**: JSON object representing the record to be created.
- **Response**: Unique ID of the created entity.

### Read

- **Endpoint**: `/read_records/`
- **Method**: GET
- **Description**: Retrieves single or multiple records from the system.
- **Query Parameters**: Optional filters to specify which records to retrieve.
- **Response**: JSON array of records.

### Delete

- **Endpoint**: `/delete_record/{id}`
- **Method**: DELETE
- **Description**: Deletes a specific record by its unique ID. Applicable only for topics and brokers.
- **Path Parameter**: `id` - Unique ID of the entity to delete.
- **Response**: Confirmation with the unique ID of the deleted entity.

## Broker Management API

### RegisterBroker

- **Endpoint**: `/register_broker/`
- **Method**: POST
- **Description**: Registers a broker when it comes online for the first time. Adds required metadata records accordingly.
- **Request Body**: Broker information.
- **Response**: Acknowledgment of registration.

### RegisterBrokerChange

- **Endpoint**: `/register_broker_change/`
- **Method**: POST
- **Description**: Updates broker information. Used for any changes to the broker's details.
- **Request Body**: Updated broker information.
- **Response**: Acknowledgment of the update.

### MetadataFetch

- **Endpoint**: `/metadata_fetch/`
- **Method**: POST
- **Description**: Acts as a heartbeat mechanism for brokers. Brokers send the last known offset of metadata, and the server responds with all changes since that offset. If the offset is too far behind, the entire metadata snapshot is sent.
- **Request Body**: Last known offset (timestamp).
- **Response**: Metadata changes since the last known offset or the entire metadata snapshot.

## Client Management API

### RegisterProducer

- **Endpoint**: `/register_producer/`
- **Method**: POST
- **Description**: Registers a new producer in the system.
- **Request Body**: Producer information.
- **Response**: Acknowledgment of registration.

### MetadataFetch

- **Endpoint**: `/metadata_fetch_client/`
- **Method**: GET
- **Description**: Fetches topics, partition, and broker information records for clients.
- **Query Parameters**: Optional filters for specific data retrieval.
- **Response**: Requested metadata information.

## Specific Endpoints

### RegisterBrokerRecord

- **Endpoints**:
  - `/register/`: Register a new broker.
  - `/brokers/`: Get all active brokers.
  - `/broker/{id}`: Get broker by ID.

### TopicRecord

- **Endpoints**:
  - `/create_topic/`: Create a new topic.
  - `/topic/{name}`: Get topic by name.

### PartitionRecord

- **Endpoints**:
  - `/create_partition/`: Create a new partition.
  - `/update_partition/`: Update partition details (add/remove replicas, increment epoch).

### ProducerIdsRecord

- **Endpoint**: `/register_producer_broker/`
- **Method**: POST
- **Description**: Registers a producer from a broker.

### BrokerRegistrationChangeBrokerRecord

- **Endpoints**:
  - `/update_broker/`: Update broker details.
  - `/unregister_broker/{id}`: Unregister a broker.

### BrokerMgmt

- **Endpoint**: `/broker_mgmt/`
- **Method**: POST
- **Description**: Returns metadata updates since a given offset/timestamp. If the offset is later than 10 minutes, the entire metadata snapshot is sent.

### ClientMgmt

- **Endpoint**: `/client_mgmt/`
- **Method**: GET
- **Description**: Returns metadata updates (topics, partitions, broker info) since a given offset/timestamp. If the offset is later than 10 minutes, the entire metadata snapshot is sent.

```
uvicorn main:app --reload
```
