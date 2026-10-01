.PHONY: check-count test coverage compose-up smoke compose-down verify

check-count:
	python scripts/check_test_count.py

test:
	pytest -ra

coverage:
	mkdir -p test-results
	pytest --cov=payments_lab --cov-report=term-missing --cov-report=xml --cov-report=html --junitxml=test-results/junit.xml

compose-up:
	docker compose up -d --build --wait

smoke:
	python scripts/compose_smoke.py

compose-down:
	docker compose down -v --remove-orphans

verify: check-count coverage
