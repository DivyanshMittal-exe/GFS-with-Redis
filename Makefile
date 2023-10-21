REDIS_PORT ?= 6379

all: workers server client

backend: redis_server rabbitmq

redis_server:
	rm dump.rdb || true
	gnome-terminal -- bash -c "redis-server --port ${REDIS_PORT}"

rabbitmq:
	gnome-terminal -- bash - c "docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management"

lua:
	cat mylib.lua | redis-cli -p ${REDIS_PORT} -x FUNCTION LOAD REPLACE

unittest:
	python -m unittest discover -s unittests

workers:
	python -m chunk_workers.worker

make server:
	python -m server

make client:
	python -m client