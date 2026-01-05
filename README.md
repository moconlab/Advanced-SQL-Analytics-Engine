# Advanced SQL Analytics Engine

A comprehensive demonstration of expert-level SQL analytics using large-scale synthetic datasets and modern data warehousing platforms (Snowflake & BigQuery).

## 🎯 Project Overview

This project showcases advanced SQL analytics techniques through:

- **Large Synthetic Dataset**: 5M+ events, 500K+ sales transactions, 50K users
- **Complex Analytics**: Window functions, sessionization, cohort analysis, funnel metrics
- **Performance Optimization**: Partitioning, clustering, indexing strategies
- **Modern Stack**: dbt models, Snowflake/BigQuery compatible SQL

## 📊 Key Features

### 1. Synthetic Data Generation
- **Sales Data**: 500,000 transactions across 2 years
- **Event Stream**: 5 million user events (page views, product views, cart actions)
- **User Data**: 50,000 users with demographic attributes
- **Product Catalog**: 1,000 products across 5 categories

### 2. Advanced Analytics Models

#### Window Functions
- Running totals and cumulative metrics
- Moving averages (7-day, 30-day)
- Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
- LAG/LEAD for time-based analysis
- FIRST_VALUE/LAST_VALUE for lifecycle tracking

#### Sessionization
- Time-based session grouping (30-minute timeout)
- Session quality scoring
- Device and traffic source analysis
- Engagement metrics

#### Cohort Analysis
- Monthly cohort grouping
- Retention rate tracking
- Customer Lifetime Value (LTV)
- Revenue per user analysis
- Cohort comparison across segments

#### Funnel Metrics
- Multi-stage conversion tracking
- Drop-off analysis at each stage
- Segment-based funnel performance
- Micro-conversion tracking

### 3. Performance Tuning
- Table partitioning strategies
- Clustering key optimization
- Materialized views
- Query optimization patterns
- Cost monitoring and analysis

## 🚀 Quick Start

### Prerequisites

- **Snowflake** OR **BigQuery** account
- **dbt** installed (`pip install dbt-snowflake` or `pip install dbt-bigquery`)
- Python 3.7+

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/moconlab/Advanced-SQL-Analytics-Engine.git
cd Advanced-SQL-Analytics-Engine
```

2. **Generate synthetic data**

For Snowflake:
```bash
# Run in Snowflake console or using SnowSQL
snowsql -f sql/data_generation/generate_synthetic_data_snowflake.sql
```

For BigQuery:
```bash
# Update project_id in the SQL file first
bq query --use_legacy_sql=false < sql/data_generation/generate_synthetic_data_bigquery.sql
```

3. **Configure dbt**

```bash
cd dbt_project
cp profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your credentials
```

4. **Install dbt dependencies**
```bash
dbt deps
```

5. **Run dbt models**
```bash
# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test
```

## 📁 Project Structure

```
Advanced-SQL-Analytics-Engine/
├── dbt_project/
│   ├── models/
│   │   ├── staging/              # Clean and standardize raw data
│   │   │   ├── stg_users.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_events.sql
│   │   │   └── stg_sales.sql
│   │   └── marts/
│   │       └── analytics/        # Analytics models
│   │           ├── window_functions_analysis.sql
│   │           ├── sessionization.sql
│   │           ├── cohort_analysis.sql
│   │           └── funnel_metrics.sql
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml
├── sql/
│   ├── data_generation/          # Synthetic data scripts
│   │   ├── generate_synthetic_data_snowflake.sql
│   │   └── generate_synthetic_data_bigquery.sql
│   └── queries/                  # Example queries
│       ├── window_functions/
│       ├── sessionization/
│       ├── cohort_analysis/
│       ├── funnel_metrics/
│       └── performance_tuning/
└── docs/
    ├── ANALYTICS_TECHNIQUES.md   # Detailed technique explanations
    └── PERFORMANCE_TUNING.md     # Performance optimization guide
```

## 📖 Documentation

### Core Concepts

- **[Analytics Techniques](docs/ANALYTICS_TECHNIQUES.md)**: Deep dive into window functions, sessionization, cohort analysis, and funnel metrics
- **[Performance Tuning](docs/PERFORMANCE_TUNING.md)**: Optimization strategies for Snowflake and BigQuery

### Example Queries

Each analytics technique includes runnable examples:

- **Window Functions**: [sql/queries/window_functions/window_functions_examples.sql](sql/queries/window_functions/window_functions_examples.sql)
- **Sessionization**: [sql/queries/sessionization/sessionization_examples.sql](sql/queries/sessionization/sessionization_examples.sql)
- **Cohort Analysis**: [sql/queries/cohort_analysis/cohort_analysis_examples.sql](sql/queries/cohort_analysis/cohort_analysis_examples.sql)
- **Funnel Metrics**: [sql/queries/funnel_metrics/funnel_metrics_examples.sql](sql/queries/funnel_metrics/funnel_metrics_examples.sql)

### Performance Optimization

Platform-specific tuning guides:

- **Snowflake**: [sql/queries/performance_tuning/snowflake_performance_tuning.sql](sql/queries/performance_tuning/snowflake_performance_tuning.sql)
- **BigQuery**: [sql/queries/performance_tuning/bigquery_performance_tuning.sql](sql/queries/performance_tuning/bigquery_performance_tuning.sql)

## 🔍 Key Queries

### Customer Lifetime Value
```sql
SELECT 
    user_id,
    purchase_date,
    net_amount,
    SUM(net_amount) OVER (
        PARTITION BY user_id 
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS lifetime_value
FROM raw_sales;
```

### Cohort Retention
```sql
SELECT 
    cohort_month,
    cohort_age_months,
    cohort_size,
    active_users,
    ROUND(100.0 * active_users / cohort_size, 2) AS retention_rate
FROM cohort_analysis
ORDER BY cohort_month, cohort_age_months;
```

### Funnel Conversion
```sql
SELECT 
    event_date,
    users_page_view,
    users_product_view,
    users_add_to_cart,
    users_purchase,
    ROUND(100.0 * users_purchase / users_page_view, 2) AS conversion_rate
FROM funnel_metrics
ORDER BY event_date DESC;
```

## 💡 Use Cases

This project demonstrates skills relevant to:

- **Data Analytics**: Customer behavior analysis, retention metrics, conversion optimization
- **Business Intelligence**: Dashboard development, KPI tracking, executive reporting
- **Data Engineering**: ETL/ELT pipelines, data modeling, performance optimization
- **Data Science**: Feature engineering for ML models, cohort-based predictions

## 🎓 Learning Outcomes

By studying this project, you'll learn:

1. **Advanced SQL Patterns**: Window functions, CTEs, complex joins, subqueries
2. **Analytics Methodologies**: Sessionization algorithms, cohort analysis, funnel tracking
3. **Performance Optimization**: Partitioning, clustering, query tuning, cost management
4. **Modern Data Stack**: dbt transformations, version control for analytics, testing
5. **Cloud Data Warehouses**: Platform-specific features of Snowflake and BigQuery

## 🛠️ Tech Stack

- **SQL**: Primary language for all analytics
- **dbt**: Data transformation and modeling
- **Snowflake**: Cloud data warehouse (option 1)
- **BigQuery**: Cloud data warehouse (option 2)
- **Python**: Minimal usage for dbt setup only

## 📈 Performance Benchmarks

On a dataset of 5M events and 500K sales:

| Query Type | Execution Time | Data Scanned | Cost |
|------------|----------------|--------------|------|
| Window Functions | 3-8 seconds | 100-300 MB | $0.001-0.003 |
| Sessionization | 15-30 seconds | 500 MB - 1 GB | $0.003-0.006 |
| Cohort Analysis | 8-15 seconds | 200-500 MB | $0.002-0.004 |
| Funnel Metrics | 10-20 seconds | 300-800 MB | $0.002-0.005 |

*Benchmarks based on BigQuery on-demand pricing. Actual costs vary by platform and configuration.*

## 🤝 Contributing

Contributions are welcome! Areas for expansion:

- Additional analytics patterns (RFM analysis, attribution modeling)
- More platform support (Redshift, Databricks)
- Real-time streaming analytics
- Machine learning integration
- Advanced visualization examples

## 📄 License

This project is open source and available under the MIT License.

## 🙏 Acknowledgments

- Inspired by real-world analytics challenges at scale
- Built with best practices from data engineering and analytics communities
- Techniques drawn from industry-leading companies (Amplitude, Mixpanel, Google Analytics)

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

**⭐ Star this repo if you find it useful!**
