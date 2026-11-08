COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: up down infra test test-integration seed demo logs

infra:
	$(COMPOSE) up -d postgres localstack api-mock

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

test:
	go test ./...

test-integration: infra
	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	POSTGRES_URL=postgres://app:app@localhost:5432/consultas \
	API_BASE_URL=http://localhost:8080 \
	go test -tags integration -count=1 ./tests/...

seed:
	$(COMPOSE) run --rm seed

logs:
	$(COMPOSE) logs -f worker scheduler
