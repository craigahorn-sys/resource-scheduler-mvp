# Resource Scheduler V2 - priority-first build

This build changes allocation behavior to:
- Priority first: Critical > High > Normal > Low
- Tie-breaker inside same priority: earliest requirement window first
- Rebalance automatically when:
  - a requirement is created
  - a pool is saved
  - a pool adjustment is added
- Includes a manual **Rebalance All Allocations** button
- Adds an **Allocation Debug** table

## Files to deploy
Upload these files to your repo root:
- app.py
- schema.sql
- requirements.txt
- README.md
- services/

## Notes
This build is intended for PostgreSQL. SQLite fallback still exists for local convenience, but the scheduling logic is designed around the PostgreSQL deployment target.
