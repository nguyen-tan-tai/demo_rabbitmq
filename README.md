# demo_rabbitmq

## Run rabbitmq docker container for queue
```
$ docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## Run rabbitmq docker container for stream 
```
$ docker run -it --rm --name rabbitmq -p 5552:5552 -p 15672:15672 -p 5672:5672 -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost' rabbitmq:4-management
```

```
$ docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream rabbitmq_stream_management
```
