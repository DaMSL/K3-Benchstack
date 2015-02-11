# cAdvisor Profiling

External profiling is handled via the cAdvisor project - a container to monitor all docker containers on the parent host. Pull the container from google/cadvisor. To include the metrics, ensure that a cadvisor container is running on each machine you want to run a benchmark experiment. Docker container names must be set so that the profiling script can scrape the JSON output from the cadvisor. 

Use the following run command:
```
docker run \
  --volume=/:/rootfs:ro \
  --volume=/var/run:/var/run:rw \
  --volume=/sys:/sys:ro \
  --volume=/var/lib/docker/:/var/lib/docker:ro \
  --publish=20080:8080 \
  --detach=true \
  --name=cadvisor \
  google/cadvisor:latest
```

Just browse to <host>:20080 to see the json. 

There are hints and run command which you can provide to the contain to fine tune & expand metrics (not included in current profiing version). 

