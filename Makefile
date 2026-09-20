.DEFAULT_GOAL := help

.PHONY: help up down logs status fault-on fault-off fault-bad verify export-case clean k8s-up k8s-down k8s-status k8s-logs

help:
	@printf '%s\n' 'make k8s-up      Deploy the Kubernetes lab' 'make k8s-status  Show lab pods and services' 'make k8s-logs    Follow application logs' 'make k8s-down    Remove the Kubernetes lab' 'make up          Start the legacy Compose lab' 'make verify      Verify the Compose stack'

k8s-up:
	kubectl apply -k deploy/kubernetes
	kubectl rollout status deployment/mysql -n fcapsule-lab --timeout=180s
	kubectl wait --for=condition=Available deployment --all -n fcapsule-lab --timeout=300s

k8s-down:
	kubectl delete -k deploy/kubernetes

k8s-status:
	kubectl get pods,svc,servicemonitor,prometheusrule -n fcapsule-lab

k8s-logs:
	kubectl logs -n fcapsule-lab -l app.kubernetes.io/part-of=fcapsule-lab --all-containers --follow --tail=80 --prefix

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
