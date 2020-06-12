![Rust](https://github.com/teivah/franz/workflows/CI/badge.svg)

# franz

# Run

## Parameters

* `-p`: port used by franz
* `-k`: kafka hosts

## Docker

```shell script
$ docker run -p 8080:8080 teivah/franz -- -p 8080 -k kafka:9092
```

## Local

```shell script
$ cargo build --release
$ ./target/release/franz -k localhost:9092 -p 8080
```

# API

## Produce

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

The response returns an 201 containing the status ID.

```json
{
    "links": [
        {
            "href": "/status/fc013d92-353e-455d-a899-bf3425521547",
            "rel": "status",
            "type": "GET"
        }
    ]
}
```

## Status

Description: Get results

Path: GET `/status/{id}`

If the job has been completed, it will return a 200:

```json
{
    "messages_sent": 100,
    "average_latency_ms": 225.9
}
```

Otherwise, it returns a 404 if the job does not exist, or a 204 if the job did not complete yet.