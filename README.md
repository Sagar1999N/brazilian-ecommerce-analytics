# Brazilian E-commerce Analytics Pipeline

[![Java](https://img.shields.io/badge/Java-11-orange.svg)](https://openjdk.java.net/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.4.0-red.svg)](https://spark.apache.org/)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A **production-grade batch ETL pipeline** implementing the **Medallion Architecture** (Bronze → Silver → Gold) for Brazilian e-commerce data analysis using Apache Spark and Java.

## 🎯 Project Objectives

This project demonstrates enterprise-level data engineering practices including:
- **Medallion Architecture** (Bronze/Silver/Gold layers)
- **Data Quality Management** with validation rules and dead letter queues
- **Idempotent Processing** for reliable reruns
- **Schema Evolution** and type-safe data modeling
- **Comprehensive Logging** and observability
- **Configuration Management** for multiple environments

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE ARCHITECTURE                    │
└─────────────────────────────────────────────────────────────────┘

📁 RAW LAYER (CSV Files)
   └── Brazilian E-commerce Dataset (Kaggle)
       ├── Orders
       ├── Customers  
       ├── Products
       └── Order Items
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER (Minimal Transformation)                       │
│  - Schema validation                                             │
│  - Raw data ingestion                                            │
│  - Parquet format                                                │
└─────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER (Cleaned & Validated)                          │
│  - Data Quality Checks ✓                                         │
│  - Deduplication                                                 │
│  - Null Handling                                                 │
│  - Data Enrichment (calculated fields)                           │
│  - Dead Letter Queue for bad records                             │
└─────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER (Business Aggregates) - Coming Soon!             │
│  - Dimensional modeling                                          │
│  - Pre-aggregated metrics                                        │
│  - Ready for BI tools                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Features Implemented

### ✅ Phase 1: Data Ingestion (Bronze Layer)
- [x] **Schema-first approach** with explicit type definitions
- [x] **Configurable data readers** for multiple file formats
- [x] **Error handling** with corrupt record capture
- [x] **Data validation** at ingestion time
- [x] **Environment-specific configurations** (dev/prod)

### ✅ Phase 2: Data Transformation (Silver Layer)
- [x] **Data Quality Rules Engine**
  - Null validation for primary keys
  - Order status validation
  - Logical date validations
  - Business rule checks
- [x] **Dead Letter Queue Pattern** - Quarantines invalid records
- [x] **Deduplication Logic** - Keeps latest records by timestamp
- [x] **Data Enrichment**
  - Delivery time calculations
  - Late delivery flags
  - Order approval time
  - Date dimension extraction (year, month, quarter, day of week)
- [x] **Idempotent Processing** - Safe to rerun multiple times
- [x] **Partitioning Strategy** - Optimized for query performance

### 🔜 Phase 3: Analytics Layer (Gold Layer) - Coming Soon
- [ ] Dimensional modeling (Fact & Dimension tables)
- [ ] Pre-aggregated business metrics
- [ ] Customer segmentation
- [ ] Product performance analysis
- [ ] Sales trends and seasonality

---

## 📊 Data Quality Framework

### Validation Rules Implemented

| Rule | Layer | Description | Action |
|------|-------|-------------|--------|
| **Null Primary Key** | Silver | Checks `order_id`, `customer_id` are not null | Quarantine |
| **Invalid Status** | Silver | Validates order status is in allowed list | Quarantine |
| **Null Timestamps** | Silver | Ensures `purchase_timestamp` is not null | Quarantine |
| **Logical Dates** | Silver | Validates delivery date ≥ purchase date | Quarantine |
| **Duplicates** | Silver | Removes duplicate `order_id`, keeps latest | Auto-fix |

### Dead Letter Queue (Quarantine)

Bad records are not lost! They are:
- Written to `/data/quarantine/` with reasons
- Partitioned by processing date
- Available for investigation and reprocessing
- Include original data + validation failure reasons

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Language** | Java | 11 |
| **Processing Engine** | Apache Spark | 3.4.0 |
| **Build Tool** | Maven | 3.8+ |
| **Logging** | SLF4J + Log4j2 | 2.20.0 |
| **Configuration** | Typesafe Config | 1.4.2 |
| **Testing** | JUnit + Mockito | 4.13.2 / 5.3.1 |

---

## 📁 Project Structure

```
brazilian-ecommerce-analytics/
├── src/main/java/com/ecommerce/
│   ├── config/                    # Configuration management
│   │   ├── AppConfig.java         # Centralized config loader
│   │   └── SparkSessionFactory.java
│   ├── schema/                    # Schema definitions
│   │   ├── OrderSchema.java
│   │   ├── CustomerSchema.java
│   │   └── ProductSchema.java
│   ├── models/                    # Data models (POJOs)
│   │   ├── Order.java
│   │   ├── Customer.java
│   │   └── Product.java
│   ├── ingestion/                 # Data ingestion (Bronze)
│   │   ├── DataReader.java
│   │   ├── DataValidator.java
│   │   └── DatasetDownloader.java
│   ├── transformation/            # Data transformation
│   │   └── silver/
│   │       └── OrdersSilverTransformer.java
│   └── jobs/                      # Spark job entry points
│       ├── DataIngestionJob.java
│       ├── SilverTransformationJob.java
│       └── SimpleTestJob.java
├── src/main/resources/
│   ├── application.conf           # Base configuration
│   ├── application-dev.conf       # Dev overrides
│   ├── application-prod.conf      # Prod overrides
│   └── log4j2.xml                 # Logging configuration
├── data/                          # Data storage (gitignored)
│   ├── raw/                       # Raw CSV files
│   ├── staging/                   # Bronze layer (parquet)
│   ├── silver/                    # Silver layer (cleaned)
│   ├── gold/                      # Gold layer (aggregates)
│   └── quarantine/                # Bad records (DLQ)
├── logs/                          # Application logs
├── pom.xml                        # Maven dependencies
└── README.md                      # This file
```

---

## 🚦 Getting Started

### Prerequisites
- Java 11 or higher
- Maven 3.8+
- 4GB+ RAM recommended

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd brazilian-ecommerce-analytics

# Build the project
mvn clean package

# Run ingestion job (Bronze layer)
mvn exec:java -Dexec.mainClass="com.ecommerce.jobs.DataIngestionJob"

# Run transformation job (Silver layer)
mvn exec:java -Dexec.mainClass="com.ecommerce.jobs.SilverTransformationJob"
```

### Configuration

Set environment using system property:
```bash
# Development (default)
mvn exec:java -Dexec.mainClass="com.ecommerce.jobs.DataIngestionJob" -Denv=dev

# Production
mvn exec:java -Dexec.mainClass="com.ecommerce.jobs.DataIngestionJob" -Denv=prod
```

---

## 📈 Performance Optimizations

1. **Partitioning Strategy**
   - Silver Orders: Partitioned by `order_year` and `order_month`
   - Improves query performance for time-based analytics

2. **Adaptive Query Execution**
   - Enabled via `spark.sql.adaptive.enabled=true`
   - Dynamically optimizes query plans

3. **Serialization**
   - Using Kryo serializer for better performance
   - Reduces memory footprint

4. **Shuffle Partitions**
   - Optimized for local development (4 partitions)
   - Can be scaled up for production clusters

---

## 🧪 Testing Strategy

### Unit Tests (Coming Soon)
- Schema validation tests
- Transformation logic tests
- Data quality rule tests

### Integration Tests (Coming Soon)
- End-to-end pipeline tests
- Data lineage validation

---

## 📊 Monitoring & Observability

### Logging Levels
- **DEBUG**: Development troubleshooting
- **INFO**: Normal operations (default)
- **WARN**: Production monitoring
- **ERROR**: Failures and exceptions

### Metrics Tracked
- Total records processed
- Valid vs invalid records
- Duplicates removed
- Processing time
- Error rates

### Log Outputs
- Console logs (development)
- Rolling file logs (`logs/app.log`)
- JSON logs for production (optional)

---

## 🎓 Key Design Patterns Implemented

1. **Medallion Architecture** - Multi-layer data lake design
2. **Idempotency** - Safe reruns with `SaveMode.Overwrite`
3. **Dead Letter Queue** - Quarantine bad records for investigation
4. **Schema Evolution** - Type-safe schema definitions
5. **Configuration Management** - Environment-specific configs
6. **Factory Pattern** - SparkSession creation
7. **Result Pattern** - Structured transformation results

---

## 📝 Data Lineage

```
Raw CSV → Bronze (Parquet) → Silver (Cleaned) → Gold (Aggregated)
            ↓                      ↓
         Schema            Data Quality
        Validation          Validation
                                ↓
                          Quarantine (DLQ)
```

---

## 🤝 Contributing

This is a personal portfolio project, but suggestions are welcome!

---

## 📄 License

This project is for educational and portfolio purposes.

---

## 📧 Contact

**Your Name**  
Data Engineer | 3 Years Experience  
📧 your.email@example.com  
💼 [LinkedIn](your-linkedin)  
💻 [GitHub](your-github)

---

## 🎯 Project Roadmap

- [x] Phase 1: Bronze Layer (Data Ingestion)
- [x] Phase 2: Silver Layer (Data Transformation)
- [ ] Phase 3: Gold Layer (Analytics)
- [ ] Phase 4: Unit & Integration Tests
- [ ] Phase 5: CI/CD Pipeline
- [ ] Phase 6: Dockerization
- [ ] Phase 7: Monitoring & Alerting
- [ ] Phase 8: Documentation & Portfolio

---

**Built with ❤️ to demonstrate production-grade data engineering skills**