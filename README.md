## Nifi Custom Redis Deduplicator Processor

### Description

 A NiFi processor that deduplicates incoming data using an existing Redis connection pool service. 

### Properties - 

- `redisConnectionPool`: Specifies the Redis connection pool service to use for connections to Redis.
- `ttl`: Indicates how long the data should exist in Redis. Setting '0 secs' would mean the data would exist forever.
- `sourceType`: Indicates type of incoming data - default is 'json'.
- `key`: Indicates json paths (comma separated list) whose value will be used as key in Redis.
- `value`: Indicates json path whose value will be used as value in Redis. It can be null as well, empty string will be stored in this case.
- `roundingInterval`: The interval in seconds used for rounding timestamps before storing data in Redis. Use '0 secs' for no rounding.
- `timestampFieldToRound`: The json path of the field containing timestamps (millis) to be rounded to the nearest interval.
- `attributeAddedForLog`: Specifies the attribute added for log purposes.


### Build

`mvn clean install`

### Setup

Add the `nifi-custom-redis-deduplicator-nar/target/nifi-custom-redis-deduplicator-nar-1.0.nar` file in the `lib/` directory of Nifi setup.

