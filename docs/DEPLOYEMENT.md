# Deployment Guide

## Local Development
```bash
# 1. Clone repository
git clone https://github.com/yourusername/brazilian-ecommerce-analytics.git
cd brazilian-ecommerce-analytics

# 2. Start services
docker-compose up -d

# 3. Run initial setup
./scripts/setup.sh

# 4. Access services:
# - Airflow: http://localhost:8080 (admin/admin)
# - PostgreSQL: localhost:5432 (ecommerce/ecommerce)
# - Metabase: http://localhost:3000