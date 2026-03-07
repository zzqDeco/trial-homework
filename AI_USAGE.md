# AI Usage

This project was developed with the assistance of an AI coding agent (Antigravity). The agent was tasked primarily with:

- Analyzing the requirement document to outline the project's task breakdown and system design.
- Creating the core Go project layout and boilerplate HTTP handlers utilizing `go-chi`.
- Generating the Dockerfile and configuring the `docker-compose.yml` components (Redis, Redpanda, Go app).
- Writing the `bash` scripts (`scripts/test_local.sh`, `scripts/test_vm.sh`, `scripts/consume.sh`) to fulfill the verification and testing scenarios.
- Providing implementations for Kafka publish (using `franz-go`) and Redis locks/idempotency handling.

All logic choices follow the project prompt constraints closely (avoiding DBs, keeping simplicity, relying strictly on environment configuration, etc.).

---

The notes above reflect the early starter-stage work. The later implementation followed a human-defined plan captured under `/plan`, and AI assistance was then used to complete the remaining system work in a structured way:

- implementing the later Postgres ingestion, projection outbox/projector flow, and dashboard API/frontend based on the manually defined design
- adding the clean-room and end-to-end validation flow, including reset, reproduction, and pipeline behavior checks
- revising the final repository documentation so the README and architecture notes match the submitted system

The final submission state is reflected by the current repository contents, `README.md`, and `ARCHITECTURE_AND_CHOICES.md`.
