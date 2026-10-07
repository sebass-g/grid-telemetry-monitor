.PHONY: help up down logs simulate

help:
	@echo "Usage: make up | make down | make logs | make simulate"

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

simulate:
	python simulator/simulate_traffic.py
