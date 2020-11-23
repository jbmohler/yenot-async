#!/bin/sh

NETWORK=test-yenot-async

start_network () {
	docker network create $NETWORK
	docker run --rm --name yenot-test-postgres --network $NETWORK -e POSTGRES_PASSWORD=mysecretpassword -d postgres
	sleep 6
	docker exec yenot-test-postgres createdb -U postgres -h localhost testdb
}

stop_network () {
	docker stop yenot-test-postgres
	docker network rm $NETWORK
}

start_yenot () {
	docker build -t yenot-async-dev -f docker/Dockerfile.yenot .
	docker run --rm -d --name yenot-server -p 8080:8080 --network $NETWORK yenot-async-dev scripts/yenotserve.py postgresql://postgres:mysecretpassword@yenot-test-postgres/testdb
	sleep 1
}

stop_yenot() {
	echo "#### Logs of yenot server ####"
	docker logs yenot-server
	echo "#### stopping the server ####"
	docker stop yenot-server
}

run_test() {
	echo "#### Now running the test ####"
	curl http://localhost:8080/api/ping
	curl http://localhost:8080/api/pingx
	curl http://localhost:8081/api/pingdb
}

#start_network
start_yenot
run_test
stop_yenot
#stop_network
