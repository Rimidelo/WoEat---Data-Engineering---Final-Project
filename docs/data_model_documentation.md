# WoEat Data Model Documentation

The WoEat data model implements a modern medallion architecture (Bronze → Silver → Gold) using Apache Iceberg tables, designed to support a food delivery platform's analytics and operational needs.

---

## 1. Bronze Layer – Raw Ingested Data

```mermaid
erDiagram
    BRONZE_ORDERS ||--o{ BRONZE_ORDER_ITEMS : contains
    BRONZE_ORDERS ||--o{ BRONZE_DRIVERS : assigned_to
    BRONZE_ORDERS ||--o{ BRONZE_RESTAURANTS : ordered_from
    BRONZE_ORDERS ||--o{ BRONZE_RATINGS : has_rating
    BRONZE_DRIVERS ||--o{ BRONZE_WEATHER : drives_in
    BRONZE_MENU_ITEMS ||--o{ BRONZE_RESTAURANTS : belongs_to

    BRONZE_ORDERS {
        string order_id
        string customer_id
        string restaurant_id
        string driver_id
        datetime order_time
        string status
        datetime delivery_time
        float total_amount
        datetime prep_start_time
        datetime prep_end_time
        float tip_amount
    }

    BRONZE_ORDER_ITEMS {
        string order_item_id
        string order_id
        string item_id
        int quantity
        float item_price
        datetime order_time
    }

    BRONZE_MENU_ITEMS {
        string item_id
        string restaurant_id
        string item_name
        string category
        float base_price
    }

    BRONZE_DRIVERS {
        string driver_id
        string name
        string zone
        datetime created_at
    }

    BRONZE_RESTAURANTS {
        string restaurant_id
        string restaurant_name
        string cuisine_type
        string zone
        boolean active_flag
        datetime created_at
    }

    BRONZE_RATINGS {
        string rating_id
        string order_id
        string driver_id
        string restaurant_id
        float driver_rating
        float food_rating
        float delivery_rating
        datetime rating_time
        string rating_type
    }

    BRONZE_WEATHER {
        datetime weather_time
        float temperature
        string condition
        string zone
    }
```


---

## 2. Silver Layer – Cleansed & Conformed Data

```mermaid
erDiagram
    SILVER_ORDERS ||--o{ SILVER_ORDER_ITEMS : contains
    SILVER_ORDERS ||--o{ SILVER_DRIVERS : assigned_to
    SILVER_ORDERS ||--o{ SILVER_RESTAURANTS : ordered_from
    SILVER_ORDERS ||--o{ SILVER_RATINGS : has_rating
    SILVER_DRIVERS ||--o{ SILVER_WEATHER : drives_in
    SILVER_MENU_ITEMS ||--o{ SILVER_RESTAURANTS : belongs_to

    SILVER_ORDERS {
        string order_id
        string customer_id
        string restaurant_id
        string driver_id
        string status
        datetime order_time
        datetime delivery_time
        float total_amount
        float prep_time_minutes
        float delivery_time_minutes
        float tip_amount
        boolean cancelled
        datetime ingest_timestamp
    }

    SILVER_ORDER_ITEMS {
        string order_item_id
        string order_id
        string item_id
        int quantity
        float item_price
        float extended_price
        datetime order_time
        datetime ingest_timestamp
    }

    SILVER_MENU_ITEMS {
        string item_id
        string restaurant_id
        string item_name
        string category
        float base_price
        datetime ingest_timestamp
    }

    SILVER_DRIVERS {
        string driver_id
        string name
        string zone
        datetime created_at
        datetime ingest_timestamp
    }

    SILVER_RESTAURANTS {
        string restaurant_id
        string restaurant_name
        string cuisine_type
        string zone
        boolean active_flag
        datetime created_at
        datetime ingest_timestamp
    }

    SILVER_RATINGS {
        string rating_id
        string order_id
        string driver_id
        string restaurant_id
        float driver_rating
        float food_rating
        float delivery_rating
        datetime rating_time
        string rating_type
        datetime ingest_timestamp
    }

    SILVER_RESTAURANT_PERFORMANCE {
        date report_date
        string restaurant_id
        float avg_prep_time
        float avg_rating
        int orders_count
        float cancel_rate
        float avg_tip
        float total_revenue
        datetime ingest_timestamp
    }

    SILVER_DRIVER_PERFORMANCE {
        date report_date
        string driver_id
        float avg_rating
        int orders_completed
        float avg_delivery_time
        float total_tips
        datetime ingest_timestamp
    }

    SILVER_WEATHER {
        string zone
        datetime weather_time
        float temperature
        string condition
        datetime ingest_timestamp
    }
```


---

## 3. Gold Layer – Star Schema for Analytics

```mermaid
erDiagram
    FACT_ORDERS ||--o{ FACT_ORDER_ITEMS : contains
    FACT_ORDERS ||--o{ DIM_DRIVERS : uses
    FACT_ORDERS ||--o{ DIM_RESTAURANTS : ordered_from
    FACT_ORDER_ITEMS ||--o{ DIM_MENU_ITEMS : references
    DIM_MENU_ITEMS ||--o{ DIM_RESTAURANTS : belongs_to
    FACT_ORDERS ||--o{ FACT_RATINGS : has_rating
    FACT_RESTAURANT_DAILY ||--o{ DIM_RESTAURANTS : performance_of
    FACT_DRIVER_DAILY ||--o{ DIM_DRIVERS : performance_of
```


---

## 4. Dimension Tables

| Table | Purpose | Grain | Key | Notable Attributes |
|-------|---------|-------|-----|--------------------|
| `dim_date` | Calendar reference for all facts | One row per calendar day | `date_key` | `is_weekend`, `is_holiday` |
| `dim_drivers` | Slowly changing record of delivery drivers | Driver × validity period (SCD-2) | `driver_key` | `zone`, `is_current`, `record_start_date`, `record_end_date` |
| `dim_restaurants` | Slowly changing record of restaurants | Restaurant × validity period (SCD-2) | `restaurant_key` | `cuisine_type`, `zone`, `active_flag` |
| `dim_menu_items` | Static menu catalog | Menu item | `menu_item_key` | `category`, `base_price`, `active_flag` |

---

## 5. Fact Tables

| Table | Grain | Foreign Keys | Measures |
|-------|-------|--------------|----------|
| `fact_orders` | One order | `date_key`, `driver_key`, `restaurant_key` | `total_amount`, `delivery_minutes`, `sla_breached`, `tip_amount` |
| `fact_order_items` | Order line | `order_key`, `menu_item_key`, `date_key` | `quantity`, `item_price`, `extended_price` |
| `fact_ratings` | One rating event | `order_key`, `driver_key`, `restaurant_key`, `date_key` | `driver_rating`, `food_rating`, `delivery_rating` |
| `fact_restaurant_daily` | Restaurant × day | `restaurant_key`, `date_key` | `avg_prep_time`, `avg_rating`, `orders_count`, `cancel_rate`, `total_revenue` |
| `fact_driver_daily` | Driver × day | `driver_key`, `date_key` | `orders_completed`, `avg_delivery_time`, `total_tips`, `total_earnings`, `hours_worked` |
| `fact_business_summary` | Organization × day | `date_key` | `total_orders`, `total_revenue`, `overall_satisfaction`, `active_drivers`, `active_restaurants` |


