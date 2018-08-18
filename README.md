Prometheus remote storage adapter for Akumuli
=============================================

This adapter integrates Akumuli with Prometheus using the remote-read and remote-write interfaces.
Adapter maintains the TCP connection pool to Akumuli. When Prometheous pushes the data adapter tries to  maintain affinity. This means that the data from the same shard will go through the same TCP connection to the TSDB. This guarantees good write performance for Akumuli.

Build Instructions
==================

Go 1.10 or greater is required to build the package. To build the package clone the repository and run `go get; go build`.

Configuration
=============

All command line parameters have meaningful defaults. Because of that,
if you're running this adapter and Akumuli on the same host you can just start adapter without parameters.
If this is not the case you have to provide `host` and `port`.

|Parameter|Value|Default|
|---------|-----|-------|
|host|Akumuli host name|localhost|
|port|Akumuli TCP endpoint port|8282|
|http-port|Akumuli HTTP API endpoint port|8181|
|write-timeout|Write timeout|10s|
|reconnect-interval|Time interval between connection attempts|5s|
|idle-time|Time interval after which idle connection should be closed|60s|

To configure Prometheus you can add this lines to your configuration file:

```
# Remote write configuration
remote_write:
  - url: "http://<adapter-host-name>:9201/write"

# Remote read configuration
remote_read:
  - url: "http://<adapter-host-name>:9201/read"
```

You can add `remote_read` or `remote_write` or both.