![CI](https://github.com/teivah/franz/workflows/CI/badge.svg)

# franz

A collection of Kafka utility tools:
* [franz-load](#franz-load): Kafka load tester.
* [franz-replicator](#franz-replicator): replicate Kafka topics.

A tiny Kafka producer load tester, written in Rust. It exposes REST endpoints to create load producer jobs and query results.

## franz-load

### Run

#### Parameters

|  Short | Long  | Description  |
|---|---|---|
| -p  | --http-port  | Kafka hosts, comma separated.  |
| -k  | --kafka-hosts  | Kafka hosts, comma separated. |

#### Docker

```shell script
$ docker run -p 8080:8080 teivah/franz-load -- -p 8080 -k kafka:9092
```

#### Local

```shell script
$ cargo build --release
$ ./target/release/franz -p 8080 -k kafka:9092 
```

### API

#### Produce

Description: Triggers a job

Path: POST `/produce`

JSON request:
* `topic`: Kafka topic
* `payload`: Kafka payload
* `expected_total`: Total number of messages
* `requested_required_acks`: Kafka required ack (-1: All, 0: None, 1: One)
* `producers`: Number of concurrent producers

JSON example:
```json
{
	"topic": "foo",
	"payload": "{true}",
	"expected_total": 100,
	"requested_required_acks": -1,
	"producers": 10 
}
```

The response returns an 201 containing an HATEAOS response with the job ID created.

```json
{
    "links": {
        "status": "/status/7bb17429-50bf-4f79-b8de-db1916683294"
    }
}
```

#### Status

Description: Get job status.

Path: GET `/status/{id}` (`id` being the one returned by `/produce`)

If the job has been completed, it will return a 200:

```json
{
    "messages_sent": 1000000,
    "average_latency_ms": 6.9
}
```

Otherwise, it returns a 404 if the job does not exist, or a 204 if the job has not completed yet.

## franz-replicator

### Run

#### Parameters

|  Short | Long  | Description  |
|---|---|---|
| -k  | --kafka-hosts  | Kafka hosts, comma separated. |
| -r  | --replication  | Replication configuration, e.g. _source1=target1;source2=target2_.  |
| -o  | --offset  | Offset: 0 (earliest) or 1 (latest).  |
| -a  | --required-acks  | Number of required acks for the producer: 0 (none), 1 (one), -1 (all).  |
| -g  | --group  | Consumer group. |
| -t  | --producer-timeout  | Producer timeout in ms. |

#### Docker

```shell script
$ docker run -p 8080:8080 teivah/franz-load -- -k kafka:9092 -r source=target -o 1 -a 0, -g consumer_group -t 3000
```

#### Local

```shell script
$ cargo build --release
$ ./target/release/franz-load -k kafka:9092 -r source=target -o 1 -a 0, -g consumer_group -t 3000
```