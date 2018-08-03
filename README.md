Prometheus remote storage adapter for Akumuli
=============================================

This application can be used to send metrics from prometheus monitoring system to Akumuli.
Adapter maintains the TCP connection pool. When Prometheous pushes the data adapter tries to provide
affinity. This means that the data from the same shard will go through the same TCP connection to the TSDB.
This guarantees good write performance for Akumuli.

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
|write-timeout|Write timeout|10s|
|reconnect-interval|Time interval between connection attempts|5s|
|idle-time|Time interval after which idle connection should be closed|60s|
