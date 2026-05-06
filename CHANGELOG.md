# Changelog

## 0.3.0

- Add Cron scheduling support (Pro) with multiple cron "groups"

## 0.2.1

- Internal Store refactors around enqueue and bulk enqueue


## 0.2.0

- Added `GET /jobs/count` (with filter params)

## 0.1.1

- Internal Store refactors (mostly around group commit)
- Restructured API internals — some log prefixes will have changed

## 0.1.0

- Initial release
- Persistent job queue with underlying LSM storage
- HTTP/1.1 and HTTP/2 API
- Streaming job dequeueing (NDJSON + MessagePack)
- Prioritised queues with FIFO ordering within same priority
- Configurable retry with exponential backoff
- Job retention polcies and automatic purging
- Unique job enqueues (Pro)
- Mutual TLS authentication (Pro)
- Interactive live queue viewer (`zizq top`)
- TLS certificate generation util (`zizq tls`)
- Online backup and restore (`zizq backup` and `zizq restore`)
- Bulk enqueue, delete, and update operations
- jq-based payload filtering for bulk operations
