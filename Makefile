REDIS_PORT ?= 6379

all: workers server client

backend: redis_server rabbitmq

redis_server:
	rm dump.rdb || true
	gnome-terminal -- bash -c "redis-server --port ${REDIS_PORT}"

rabbitmq:
	gnome-terminal -- bash -c "sudo docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management"

lua:
	cat mylib.lua | redis-cli -p ${REDIS_PORT} -x FUNCTION LOAD REPLACE

unittest:
	python -m unittest discover -s unittests

workers:
	python -m chunk_workers.worker

server:
	python -m server

client:
	python -m client

health:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 GFS --count --max-complexity=11 --max-line-length=127 --statistics
	mypy GFS