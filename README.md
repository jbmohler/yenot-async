# Intro

See https://github.com/jbmohler/yenot; make it async

# Development

The easiest development flow is to user docker.

This builds and runs the server against a ephemeral postgresql database.
Browse to http://localhost:8080/api/ping for a simple test.

```sh
  test-interactive.sh
```


```sh
  docker build -t yenot-async-dev -f docker/Dockerfile.yenot .
  docker run --rm --name yenot-test-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
  sleep 6
  docker exec yenot-test-postgres createdb -U postgres -h localhost my_coverage_test
  YENOT_DB_URL=postgresql://postgres:mysecretpassword@localhost/my_coverage_test sh full-coverage.sh
  docker stop yenot-test-postgres
```
