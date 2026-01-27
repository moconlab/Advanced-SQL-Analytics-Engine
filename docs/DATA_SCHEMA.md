# Data Schema Documentation

This document describes the data model and relationships in the Advanced SQL Analytics Engine.

## Entity Relationship Diagram

```
┌─────────────────┐
│   raw_users     │
├─────────────────┤
│ user_id (PK)    │
│ user_email      │
│ age             │
│ age_group       │
│ region          │
│ device_type     │
│ signup_date     │
│ cohort_month    │
└────────┬────────┘
         │
         │ 1:N
         │
    ┌────┴───────────────────┐
    │                        │
    │                        │
┌───▼──────────┐      ┌──────▼─────────┐
│  raw_events  │      │   raw_sales    │
├──────────────┤      ├────────────────┤
│ event_id(PK) │      │ sale_id (PK)   │
│ user_id (FK) │      │ user_id (FK)   │
│ product_id   │      │ product_id (FK)│
│ event_type   │      │ purchase_date  │
│ event_date   │      │ quantity       │
│ traffic_src  │      │ net_amount     │
└──────┬───────┘      └────────┬───────┘
       │                       │
       │                       │
       │ N:1                   │ N:1
       │                       │
    ┌──┴───────────────────────┴──┐
    │      raw_products            │
    ├──────────────────────────────┤
    │ product_id (PK)              │
    │ product_name                 │
    │ category                     │
    │ brand                        │
    │ current_price                │
    └──────────────────────────────┘
```

## Table Descriptions

### Raw Layer Tables

#### `raw_users`
User dimension table containing customer information.

| Column | Type | Description |
|--------|------|-------------|
| user_id | INTEGER | Primary key |
| user_email | VARCHAR | Email address |
| age | INTEGER | User age |
| age_group | VARCHAR | Age group (18-24, 25-34, etc.) |
| region | VARCHAR | Geographic region |
| device_type | VARCHAR | Primary device (Mobile, Desktop, Tablet) |
| signup_date | DATE | Date user signed up |
| cohort_month | DATE | First day of signup month (for cohort analysis) |

**Size:** 50,000 rows

#### `raw_products`
Product catalog dimension table.

| Column | Type | Description |
|--------|------|-------------|
| product_id | INTEGER | Primary key |
| product_name | VARCHAR | Product name |
| category | VARCHAR | Product category |
| brand | VARCHAR | Product brand |
| base_price | DECIMAL | Base price |
| current_price | DECIMAL | Current selling price |

**Size:** 1,000 rows

#### `raw_events`
Event stream fact table capturing user interactions.

| Column | Type | Description |
|--------|------|-------------|
| event_id | INTEGER | Primary key |
| user_id | INTEGER | Foreign key to users |
| product_id | INTEGER | Related product (if applicable) |
| event_type | VARCHAR | Type: page_view, product_view, add_to_cart, remove_from_cart |
| event_timestamp | TIMESTAMP | When event occurred |
| event_date | DATE | Date of event (for partitioning) |
| session_duration_seconds | INTEGER | Duration of page view |
| traffic_source | VARCHAR | organic, paid_search, social, direct |
| event_properties | VARIANT/JSON | Additional event metadata |

**Size:** 5,000,000 rows
**Partitioning:** By event_date
**Clustering:** By user_id, event_type

#### `raw_sales`
Sales transactions fact table.

| Column | Type | Description |
|--------|------|-------------|
| sale_id | INTEGER | Primary key |
| user_id | INTEGER | Foreign key to users |
| product_id | INTEGER | Foreign key to products |
| purchase_timestamp | TIMESTAMP | When purchase occurred |
| purchase_date | DATE | Date of purchase (for partitioning) |
| quantity | INTEGER | Number of units purchased |
| current_price | DECIMAL | Price per unit at time of sale |
| total_amount | DECIMAL | Subtotal (quantity * price) |
| discount_amount | DECIMAL | Discount applied |
| net_amount | DECIMAL | Final amount after discount |
| payment_method | VARCHAR | credit_card, paypal, bank_transfer |
| order_status | VARCHAR | completed, refunded |

**Size:** 500,000 rows
**Partitioning:** By purchase_date
**Clustering:** By user_id, product_id

### Staging Layer Models

Staging models clean and standardize raw data:

- `stg_users`: Cleaned user data
- `stg_products`: Cleaned product catalog
- `stg_events`: Cleaned event stream
- `stg_sales`: Cleaned sales (completed orders only)

**Transformations:**
- Remove null keys
- Filter to valid records
- Add loaded_at timestamp
- Standardize column names
- Standardize dates
- Add currency column

### Analytics Layer Models

#### `window_functions_analysis`
Demonstrates various window function patterns.

**Key Metrics:**
- Running totals (lifetime value)
- Rankings (product, category)
- Moving averages (7-day, 30-day)
- Purchase intervals
- Percentiles and quartiles

**Grain:** One row per purchase transaction

#### `sessionization`
Groups events into user sessions.

**Algorithm:**
- Session timeout: 30 minutes
- New session starts if gap > 30 minutes or first event

**Key Metrics:**
- Session duration
- Events per session
- Session quality score
- Engagement metrics

**Grain:** One row per user session

#### `cohort_analysis`
Tracks monthly user cohorts over time.

**Cohort Definition:** Users grouped by signup month

**Key Metrics:**
- Retention rate by cohort age
- Revenue per cohort
- Average revenue per user (ARPU)
- Customer lifetime value (LTV)

**Grain:** One row per cohort-month-age combination

#### `funnel_metrics`
Daily conversion funnel metrics.

**Funnel Stages:**
1. Page View
2. Product View
3. Add to Cart
4. Purchase

**Key Metrics:**
- Users at each stage
- Stage-to-stage conversion rates
- Overall conversion rate
- Drop-off percentages

**Grain:** One row per date-segment combination

## Data Lineage

```
raw_users ────────┐
                  ├──> stg_users ────────┐
                  │                      │
raw_products ─────┤                      │
                  ├──> stg_products ──────┼──> window_functions_analysis
                  │                      │
raw_events ───────┤                      │
                  ├──> stg_events ────────┼──> sessionization
                  │                      │
raw_sales ────────┘                      │
                  └──> stg_sales ─────────┼──> cohort_analysis
                                          │
                                          └──> funnel_metrics
```

## Partitioning and Clustering Strategy

### Snowflake

**Clustering Keys:**
- `raw_events`: (event_date, user_id)
- `raw_sales`: (purchase_date, user_id)

**Benefits:**
- Faster filters on date and user
- Improved join performance
- Automatic maintenance (Enterprise Edition)

### BigQuery

**Partitioning:**
- `raw_events`: PARTITION BY DATE(event_timestamp)
- `raw_sales`: PARTITION BY DATE(purchase_timestamp)

**Clustering:**
- `raw_events`: CLUSTER BY user_id, event_type
- `raw_sales`: CLUSTER BY user_id, product_id

**Benefits:**
- Significant cost savings through partition pruning
- Faster queries on clustered columns
- Reduced bytes scanned

## Data Refresh Strategy

### Full Refresh
All tables are recreated:
```bash
dbt run --full-refresh
```

### Incremental Refresh
Can be configured for fact tables:
```yaml
{{ config(
    materialized='incremental',
    unique_key='sale_id'
) }}
```

### Recommended Schedule

| Model Type | Refresh Frequency | Method |
|------------|-------------------|--------|
| Staging | Daily | Full refresh |
| Analytics | Daily | Full refresh |
| Large fact tables | Hourly | Incremental |
| Dimensions | Weekly | Full refresh |

## Data Quality Checks

### dbt Tests

**Uniqueness:**
- All primary keys (user_id, product_id, event_id, sale_id)

**Not Null:**
- All foreign keys
- Critical dimension attributes

**Referential Integrity:**
- event.user_id -> users.user_id
- event.product_id -> products.product_id
- sale.user_id -> users.user_id
- sale.product_id -> products.product_id

**Custom Tests:**
- Retention rates between 0 and 100
- Dates within expected range
- Revenue values are positive

Run tests:
```bash
dbt test
```

## Performance Considerations

### Query Patterns

**Optimized:**
- Filter on partition columns (month, date)
- Filter on clustering columns (user_id, product_id)
- Select only needed columns
- Predicate pushdown (filter before join)

**To Avoid:**
- Full table scans without date filter
- SELECT * on large tables
- Unbounded window functions
- Cartesian joins

### Size Estimates

| Table | Rows | Columns | Size (uncompressed) |
|-------|------|---------|---------------------|
| raw_users | 50K | 8 | ~5 MB |
| raw_products | 1K | 6 | ~500 KB |
| raw_events | 5M | 10 | ~500 MB |
| raw_sales | 500K | 12 | ~50 MB |

**Total:** ~555 MB uncompressed, ~100-150 MB compressed

## Extension Points

### Adding New Dimensions
1. Create generation script in `sql/data_generation/`
2. Add staging model in `dbt_project/models/staging/`
3. Join to fact tables in analytics models

### Adding New Metrics
1. Add calculations to existing models, or
2. Create new model in `dbt_project/models/marts/analytics/`
3. Document in schema.yml

### Supporting More Platforms
1. Create platform-specific data generation script
2. Update dbt profiles for new platform
3. Test platform-specific SQL syntax
