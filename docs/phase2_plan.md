# Phase 2: End-to-End Object PUT and GET with Single Worker

Implement Phase 2 of the DistributedFS project, focusing on establishing a complete end-to-end object storage workflow between the client, controller, and a single worker.

The Worker implementation and its containerization are already complete. This phase should build upon the existing Worker functionality rather than redesigning or reimplementing it.

## Objectives

### 1. Client-facing API

Expose APIs through the Controller that allow clients to upload and download objects.

- Implement an object upload (`PUT`) endpoint that accepts raw object data from the client.
- Implement an object download (`GET`) endpoint that accepts an object ID and returns the original object data.
- The client should only provide object data during upload. No custom metadata, replication settings, or storage policies are required at this stage.
- Ensure appropriate HTTP status codes and error responses.

### 2. Controller–Worker orchestration

Implement the Controller's orchestration logic using the existing Worker internal APIs.

**Upload flow:**

- Receive object data from the client.
- Process the object incrementally by chunking it, without loading the entire object into memory.
- Send chunks to the single available Worker using its existing internal API.
- Collect the information returned by the Worker for each successfully stored chunk.
- Generate an object ID and associate the object with its corresponding chunk information.
- Persist the object metadata only after the necessary chunk storage operations have completed successfully.
- Return the object ID to the client.

**Download flow:**

- Accept an object ID from the client.
- Retrieve its persisted metadata.
- Use the chunk information to retrieve the corresponding chunks from the Worker.
- Fetch chunks in the correct sequence and reconstruct the original object.
- Stream the reconstructed data back to the client without unnecessarily buffering the entire object in memory.

### 3. Metadata persistence

Implement persistent metadata storage in the Controller.

- Persist object ID and associated object information.
- Persist chunking information, including chunk sequence numbers and the corresponding chunk IDs.
- Ensure metadata survives Controller restarts and container recreation when the Controller's persistent volume is retained.
- Maintain consistency between stored metadata and successfully stored chunks.
- Support retrieving object metadata during GET requests.

### 4. Docker deployment and networking

Integrate the Controller and Worker into a functional Docker-based deployment.

- Ensure the Controller and Worker communicate over the Docker network using service/container networking rather than relying on localhost.
- Ensure the client can access the Controller through its exposed endpoint.
- Configure persistent volumes for Controller metadata and Worker chunk data.
- Verify that the complete upload and download workflow works when all services are containerized.
- Ensure the deployment can be brought up and down consistently without losing persisted data when volumes are retained.

###

## Scope exclusions

Do not implement the following in this phase:

- Multiple-worker distribution or placement policies.
- Replication or redundant chunk storage.
- Worker failure detection, heartbeats, or health-based management.
- Automatic replication or recovery of lost chunks.
- Client-provided custom metadata.
- Advanced load balancing or storage optimization.

The focus of this phase is a reliable, functional end-to-end PUT and GET workflow with a single Worker, persistent metadata, chunk-based storage, and working containerized networking.

The implementation should integrate cleanly with the existing project and preserve the current Worker functionality.


