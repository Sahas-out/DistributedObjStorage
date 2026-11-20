docker build -f worker/Dockerfile -t dos-worker .
docker run -d --name worker1 -p 8081:8080 -e WORKER_ID=worker1 -v worker1-data:/data dos-worker
