REDIS_PORT ?= 6379

redis_server:
	rm dump.rdb || true
	redis-server --port ${REDIS_PORT}

lua:
	cat mylib.lua | redis-cli -p ${REDIS_PORT} -x FUNCTION LOAD REPLACE

unittest:
	python -m unittest discover -s unittests
