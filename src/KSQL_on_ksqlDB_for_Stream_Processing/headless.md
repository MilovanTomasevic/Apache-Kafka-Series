
Edit the KSQL server config file `ksql-server.properties` (eg., `/opt/confluent/etc/ksql/ksql-server.properties` ) and add these 3 lines to the end of the configuration file

```
ksql.security.extension.class=io.confluent.ksql.rest.server.security.KsqlDefaultSecurityExtension
ksql.sink.partitions=1
ksql.sink.replicas=1
```
