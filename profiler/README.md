#Profiling

(this is a work in progress)

This is configured to run on mddb2 only -- it connects to a postgres container. To use it simply invoke the profile.sh bash script:
```
profile.sh orcl start
```

or replace 'orcl' with the container name you which to profile. When you're done, invoke:
```
profile.sh stop
```


To view the full results:

```

docker exec -it postgres /usr/lib/postgresql/9.4/bin/psql -U postgres -d data --pset pager=off -c 'SELECT * FROM cadvisor WHERE run_id={{your run id}}'

```

The initial script should output your run ID for the profiling.
