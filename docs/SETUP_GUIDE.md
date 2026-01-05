# Setup Guide

This guide will walk you through setting up the Advanced SQL Analytics Engine on your chosen platform.

## Prerequisites

### Required
- A cloud data warehouse account (choose one):
  - **Snowflake** (free trial available)
  - **BigQuery** (GCP free tier available)
- **Python 3.7+**
- **pip** (Python package manager)
- **git**

### Recommended
- SQL client (DBeaver, DataGrip, or platform-specific clients)
- Code editor (VS Code with SQL extensions)

## Platform-Specific Setup

### Option 1: Snowflake Setup

#### 1. Create Snowflake Account
1. Sign up at [snowflake.com](https://signup.snowflake.com/)
2. Choose cloud provider and region
3. Note your account identifier

#### 2. Set Up Database and Warehouse
```sql
-- Connect to Snowflake and run these commands
USE ROLE ACCOUNTADMIN;

-- Create database
CREATE DATABASE IF NOT EXISTS ANALYTICS;
USE DATABASE ANALYTICS;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Create role (optional)
CREATE ROLE IF NOT EXISTS ANALYST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;
GRANT ALL ON DATABASE ANALYTICS TO ROLE ANALYST;
GRANT ROLE ANALYST TO USER <your_username>;
```

#### 3. Generate Synthetic Data
```bash
# Option A: Using SnowSQL
snowsql -a <account> -u <username> -d ANALYTICS -s PUBLIC \
    -f sql/data_generation/generate_synthetic_data_snowflake.sql

# Option B: Copy/paste into Snowflake Web UI
# Open sql/data_generation/generate_synthetic_data_snowflake.sql
# Copy contents and run in Snowflake worksheet
```

#### 4. Configure Environment Variables
```bash
export SNOWFLAKE_ACCOUNT="<your_account>"
export SNOWFLAKE_USER="<your_username>"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_ROLE="ANALYST"
export SNOWFLAKE_DATABASE="ANALYTICS"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

#### 5. Install dbt for Snowflake
```bash
pip install dbt-snowflake
```

---

### Option 2: BigQuery Setup

#### 1. Create GCP Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create new project or select existing
3. Enable BigQuery API
4. Note your project ID

#### 2. Set Up Service Account
```bash
# In GCP Console:
# 1. Go to IAM & Admin > Service Accounts
# 2. Create Service Account with these roles:
#    - BigQuery Admin
#    - BigQuery Data Editor
# 3. Create and download JSON key
# 4. Save as ~/bigquery-key.json
```

#### 3. Create Dataset
```sql
-- In BigQuery Console
CREATE SCHEMA `<project_id>.analytics`
OPTIONS(
  location="US",
  description="Advanced SQL Analytics Engine"
);
```

#### 4. Generate Synthetic Data
```bash
# Update project_id in the SQL file first
sed -i 's/{{ project_id }}/<your_project_id>/g' \
    sql/data_generation/generate_synthetic_data_bigquery.sql

# Run the script
bq query --use_legacy_sql=false \
    < sql/data_generation/generate_synthetic_data_bigquery.sql
```

#### 5. Configure Environment Variables
```bash
export GCP_PROJECT="<your_project_id>"
export GCP_KEYFILE_PATH="$HOME/bigquery-key.json"
```

#### 6. Install dbt for BigQuery
```bash
pip install dbt-bigquery
```

---

## dbt Configuration

### 1. Clone Repository
```bash
git clone https://github.com/moconlab/Advanced-SQL-Analytics-Engine.git
cd Advanced-SQL-Analytics-Engine
```

### 2. Configure dbt Profile

#### For Snowflake:
```bash
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
advanced_sql_analytics:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: ${SNOWFLAKE_ACCOUNT}
      user: ${SNOWFLAKE_USER}
      password: ${SNOWFLAKE_PASSWORD}
      role: ${SNOWFLAKE_ROLE}
      database: ${SNOWFLAKE_DATABASE}
      warehouse: ${SNOWFLAKE_WAREHOUSE}
      schema: public
      threads: 4
      client_session_keep_alive: False
EOF
```

#### For BigQuery:
```bash
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
advanced_sql_analytics:
  target: bigquery
  outputs:
    bigquery:
      type: bigquery
      method: service-account
      project: ${GCP_PROJECT}
      dataset: analytics
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
      keyfile: ${GCP_KEYFILE_PATH}
EOF
```

### 3. Install dbt Dependencies
```bash
cd dbt_project
dbt deps
```

### 4. Test Connection
```bash
dbt debug
```

Expected output:
```
Connection test: OK
All checks passed!
```

### 5. Run dbt Models
```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_users

# Run with full refresh
dbt run --full-refresh
```

### 6. Test Data Quality
```bash
# Run all tests
dbt test

# Test specific model
dbt test --select stg_users
```

## Verification

### 1. Check Data Generation
```sql
-- Snowflake
SELECT 'Users' AS table_name, COUNT(*) AS row_count FROM raw_users
UNION ALL
SELECT 'Products', COUNT(*) FROM raw_products
UNION ALL
SELECT 'Events', COUNT(*) FROM raw_events
UNION ALL
SELECT 'Sales', COUNT(*) FROM raw_sales;

-- BigQuery
SELECT 'Users' AS table_name, COUNT(*) AS row_count 
FROM `<project_id>.analytics.raw_users`
UNION ALL
SELECT 'Products', COUNT(*) 
FROM `<project_id>.analytics.raw_products`
UNION ALL
SELECT 'Events', COUNT(*) 
FROM `<project_id>.analytics.raw_events`
UNION ALL
SELECT 'Sales', COUNT(*) 
FROM `<project_id>.analytics.raw_sales`;
```

Expected results:
- Users: 50,000
- Products: 1,000
- Events: 5,000,000
- Sales: 500,000

### 2. Check dbt Models
```bash
dbt list
```

You should see:
- 4 staging models
- 4 analytics models

### 3. Query Analytics Models
```sql
-- Check window functions analysis
SELECT * FROM window_functions_analysis LIMIT 10;

-- Check sessionization
SELECT * FROM sessionization LIMIT 10;

-- Check cohort analysis
SELECT * FROM cohort_analysis LIMIT 10;

-- Check funnel metrics
SELECT * FROM funnel_metrics LIMIT 10;
```

## Running Example Queries

### 1. Window Functions
```bash
# Snowflake
snowsql -f sql/queries/window_functions/window_functions_examples.sql

# BigQuery
bq query --use_legacy_sql=false \
    < sql/queries/window_functions/window_functions_examples.sql
```

### 2. Sessionization
```bash
# Snowflake
snowsql -f sql/queries/sessionization/sessionization_examples.sql

# BigQuery
bq query --use_legacy_sql=false \
    < sql/queries/sessionization/sessionization_examples.sql
```

### 3. Cohort Analysis
```bash
# Snowflake
snowsql -f sql/queries/cohort_analysis/cohort_analysis_examples.sql

# BigQuery
bq query --use_legacy_sql=false \
    < sql/queries/cohort_analysis/cohort_analysis_examples.sql
```

### 4. Funnel Metrics
```bash
# Snowflake
snowsql -f sql/queries/funnel_metrics/funnel_metrics_examples.sql

# BigQuery
bq query --use_legacy_sql=false \
    < sql/queries/funnel_metrics/funnel_metrics_examples.sql
```

## Troubleshooting

### dbt Connection Issues

**Problem:** `dbt debug` fails
**Solutions:**
1. Verify credentials in `~/.dbt/profiles.yml`
2. Check environment variables are set
3. Test database connection manually
4. Ensure IP is whitelisted (if required)

### Data Generation Issues

**Problem:** Synthetic data scripts fail
**Solutions:**
1. Check you have CREATE TABLE permissions
2. Verify database and schema exist
3. For BigQuery: Ensure dataset exists and project ID is correct
4. For Snowflake: Ensure warehouse is running

### dbt Run Failures

**Problem:** `dbt run` fails on specific models
**Solutions:**
1. Run with `--full-refresh` flag
2. Check source tables exist: `SELECT * FROM raw_users LIMIT 1`
3. Review error message for specific SQL issues
4. Run models individually to isolate issue

### Performance Issues

**Problem:** Queries are slow
**Solutions:**
1. Check clustering is applied: See performance_tuning docs
2. Verify partitioning is working
3. Review warehouse/slot usage
4. Reduce data volume for testing

## Next Steps

1. **Explore Analytics**: Run example queries to understand patterns
2. **Customize Models**: Modify dbt models for your use cases
3. **Add Visualizations**: Connect Tableau, Looker, or other BI tools
4. **Optimize Performance**: Apply clustering and partitioning strategies
5. **Extend Datasets**: Add more synthetic data or connect real data sources

## Resources

### Documentation
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)

### Learning Resources
- [Advanced SQL Techniques](docs/ANALYTICS_TECHNIQUES.md)
- [Performance Tuning Guide](docs/PERFORMANCE_TUNING.md)

### Community
- [dbt Slack Community](https://www.getdbt.com/community/)
- [Snowflake Community](https://community.snowflake.com/)
- [BigQuery Stack Overflow](https://stackoverflow.com/questions/tagged/google-bigquery)

## Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review documentation in `docs/` folder
3. Open an issue on GitHub

---

**Happy Analyzing! 📊**
