docker build -f worker/Dockerfile -t dos-worker .
# A worker needs a running controller to register with (here on the host network).
docker run -d --name worker-standalone -p 8081:8080 -e CONTROLLER_URL=http://host.docker.internal:8080 --add-host=host.docker.internal:host-gateway -v worker-standalone-data:/data dos-worker
