# Architecture Decision Record (ADR)

## ADR 1: Tech Stack Selection
**Date:** 2024-01-15  
**Status:** Accepted  

### Context
Need to choose technologies for Brazilian e-commerce ETL pipeline.

### Decision
- **Processing:** Apache Spark 3.4 (Java API)
- **Orchestration:** Apache Airflow 2.6
- **Data Warehouse:** PostgreSQL 14
- **Containerization:** Docker + Docker Compose
- **CI/CD:** GitHub Actions

### Consequences
- Spark provides distributed processing for large datasets
- Airflow offers robust scheduling and monitoring
- PostgreSQL is free, ACID-compliant, supports complex queries
- Docker ensures environment consistency

## ADR 2: Data Storage Strategy
**Date:** 2024-01-15  
**Status:** Accepted  

### Context
How to handle raw, staging, and processed data.

### Decision
Three-layer architecture:
1. **Raw Layer:** Original CSVs (immutable)
2. **Staging Layer:** Cleaned, validated data
3. **Processed Layer:** Aggregated business data

### Consequences
- Raw data preserved for audit
- Staging enables reprocessing
- Processed optimized for queries