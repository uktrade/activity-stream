SHELL = /bin/sh

start-dev:
	docker-compose up --build --force-recreate -d

stop-dev:
	docker-compose down