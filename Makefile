.DEFAULT_GOAL := help

.PHONY: help up down logs status fault-on fault-off fault-bad verify export-case clean

help:
	@printf '%s\n' 'make up          Start the five-container lab' 'make fault-on    Enable database lock contention' 'make fault-bad   Enable a bad database configuration failure' 'make fault-off   Recover inventory' 'make logs        Follow all structured logs' 'make verify      Verify health, Prometheus, and log throughput' 'make export-case Export a bounded case for FCAPSule' 'make down        Stop the lab'

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs --follow --tail=100

status:
	docker compose ps

fault-on:
	python3 tools/control.py lock-contention

fault-bad:
	python3 tools/control.py bad-database-config

fault-off:
	python3 tools/control.py normal

verify:
	python3 tools/verify_stack.py

export-case:
	python3 tools/export_case.py --out artifacts/$$(date -u +lab-%Y%m%dT%H%M%SZ)

clean:
	docker compose down --volumes --remove-orphans
