.PHONY: local-test test-negative test-catalogue test-injection test-stack acl-init app all logs logs-test \
        stop-app stop-test clean clean-all monitor topics demo-diag demo-denied check

# ---- Offline unit test (no Docker, no Kafka, no LLM) ----

local-test:
	python3 tests/test_deterministic_flow.py

# ---- Live tests (require the Docker stack — see test-stack / app below) ----

test-negative:
	python3 tests/test_denied_mutations.py

test-catalogue:
	python3 tests/test_tool_catalogue.py

test-injection:
	python3 tests/test_active_injection.py

# ---- Kafka test stack (SASL/PLAIN + StandardAuthorizer + ACLs) ----

test-stack:
	docker network create kafka-ops-agent-governance-test 2>/dev/null || true
	docker compose -f docker-compose.test.yml up -d
	@echo "Test Kafka ready on port 9093 (SASL_PLAINTEXT/PLAIN)"
	@echo "Kafka UI ready at http://localhost:8081"
	@echo "ACLs for diagnostic-agent-ro applied by the kafka-acl-init service (see its logs below)"

# Re-apply ACLs without a full stack restart — safe to re-run, kafka-acls.sh --add is idempotent.
acl-init:
	docker compose -f docker-compose.test.yml up kafka-acl-init

app:
	docker compose -f docker-compose.app.yml up -d

all: test-stack
	@sleep 5
	docker compose -f docker-compose.app.yml up -d

logs:
	docker compose -f docker-compose.app.yml logs -f

logs-test:
	docker compose -f docker-compose.test.yml logs -f

stop-app:
	docker compose -f docker-compose.app.yml down

stop-test:
	docker compose -f docker-compose.test.yml down

clean:
	docker compose -f docker-compose.test.yml down -v
	docker compose -f docker-compose.app.yml down

clean-all: clean
	docker network rm kafka-ops-agent-governance-test 2>/dev/null || true

monitor:
	docker compose -f docker-compose.app.yml logs -f & \
	docker compose -f docker-compose.test.yml logs -f kafka-ui & \
	wait

topics:
	docker compose -f docker-compose.test.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 --command-config /etc/kafka/admin.properties --describe

# ---- Demo scripts ----

demo-diag:
	@echo "=== Demo: Diagnostic (poison message -> diagnosis -> proposed fix) ==="
	./scripts/demo-diag.sh

demo-denied:
	@echo "=== Demo: Denied mutations (diagnostic-agent-ro identity vs the broker) ==="
	./scripts/demo-denied.sh

# ---- Pipeline health check ----

check:
	@echo "=== Pipeline Health Check ==="
	@echo ""
	@echo "--- Consumer Groups ---"
	@docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9093 --command-config /etc/kafka/admin.properties --list 2>/dev/null | while read group; do \
		echo ""; \
		echo "Group: $$group"; \
		docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9093 --command-config /etc/kafka/admin.properties --group "$$group" --describe 2>/dev/null | tail -n +3; \
	done
	@echo ""
	@echo "--- Topic Message Counts ---"
	@for topic in factures incidents; do \
		count=$$(docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server kafka:9093 --command-config /etc/kafka/admin.properties --topic $$topic --time -1 2>/dev/null | awk -F: '{sum+=$$NF} END{print sum+0}'); \
		echo "  $$topic: $$count messages"; \
	done
	@echo ""
	@echo "--- ACL bindings for diagnostic-agent-ro ---"
	@docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9093 --command-config /etc/kafka/admin.properties --list --principal User:diagnostic-agent-ro 2>/dev/null
	@echo ""
	@echo "--- Recent Diagnostic Activity (last 10) ---"
	@docker compose -f docker-compose.app.yml logs --tail=200 2>/dev/null | grep -i "incident produced\|tool catalogue" | tail -10 || echo "  (no diagnostic activity yet)"
