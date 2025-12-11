# Data Dictionary

## Table: fact_orders
| Column | Type | Description | Source | Transformation |
|--------|------|-------------|--------|----------------|
| order_id | VARCHAR(50) | Unique order identifier | olist_orders_dataset | Direct mapping |
| customer_id | VARCHAR(50) | Customer identifier | olist_orders_dataset | FK to dim_customers |
| order_status | VARCHAR(20) | Order current status | olist_orders_dataset | ENUM: delivered, shipped, etc |
| order_purchase_timestamp | TIMESTAMP | When order was placed | olist_orders_dataset | Convert from string |
| order_approved_at | TIMESTAMP | When payment approved | olist_orders_dataset | NULL if not approved |
| order_delivered_carrier_date | TIMESTAMP | When handed to carrier | olist_orders_dataset | NULL if not shipped |
| order_delivered_customer_date | TIMESTAMP | When delivered to customer | olist_orders_dataset | NULL if not delivered |
| order_estimated_delivery_date | TIMESTAMP | Estimated delivery date | olist_orders_dataset | Business expectation |

## Table: dim_customers
| Column | Type | Description | Source | Transformation |
|--------|------|-------------|--------|----------------|
| customer_id | VARCHAR(50) | Unique customer identifier | olist_customers_dataset | Primary key |
| customer_unique_id | VARCHAR(50) | Anonymous customer ID | olist_customers_dataset | For tracking |
| customer_zip_code_prefix | VARCHAR(10) | ZIP code prefix | olist_customers_dataset | First 5 digits |
| customer_city | VARCHAR(100) | Customer city | olist_customers_dataset | Title case |
| customer_state | VARCHAR(2) | Brazilian state code | olist_customers_dataset | Uppercase |