# WoEat Data Model

## Overview
The WoEat platform data model follows a star schema design with fact and dimension tables organized across bronze, silver, and gold layers.

## Bronze Layer (Raw Data)
Raw data ingested from various sources without transformation.

## Silver Layer (Cleaned Data)
Cleaned and validated data with proper data types and quality checks.

## Gold Layer (Business Ready)
Aggregated and business-ready data for analytics and reporting.

## Entity Relationship Diagram

```mermaid
erDiagram
    %% Dimension Tables
    DIM_CUSTOMERS {
        string customer_id PK
        string customer_name
        string email
        string phone
        string address
        string city
        string registration_date
        string customer_status
        timestamp created_at
        timestamp updated_at
        int scd_version
        boolean is_current
    }
    
    DIM_RESTAURANTS {
        string restaurant_id PK
        string restaurant_name
        string cuisine_type
        string address
        string city
        float rating
        string phone
        string owner_name
        timestamp created_at
        timestamp updated_at
        int scd_version
        boolean is_current
    }
    
    DIM_DRIVERS {
        string driver_id PK
        string driver_name
        string phone
        string license_number
        string vehicle_type
        float rating
        string status
        timestamp created_at
        timestamp updated_at
        int scd_version
        boolean is_current
    }
    
    DIM_MENU_ITEMS {
        string item_id PK
        string restaurant_id FK
        string item_name
        string category
        float price
        string description
        boolean is_available
        timestamp created_at
        timestamp updated_at
    }
    
    DIM_DATE {
        string date_id PK
        date date_value
        int year
        int month
        int day
        int quarter
        string day_name
        string month_name
        boolean is_weekend
        boolean is_holiday
    }
    
    %% Fact Tables
    FACT_ORDERS {
        string order_id PK
        string customer_id FK
        string restaurant_id FK
        string driver_id FK
        string date_id FK
        timestamp order_timestamp
        timestamp pickup_timestamp
        timestamp delivery_timestamp
        float order_amount
        float delivery_fee
        float tip_amount
        float total_amount
        string order_status
        float delivery_distance
        int delivery_time_minutes
        string payment_method
    }
    
    FACT_ORDER_ITEMS {
        string order_item_id PK
        string order_id FK
        string item_id FK
        int quantity
        float unit_price
        float total_price
        string special_instructions
    }
    
    FACT_DRIVER_LOCATIONS {
        string location_id PK
        string driver_id FK
        timestamp timestamp
        float latitude
        float longitude
        string status
        float speed
    }
    
    %% Relationships
    DIM_CUSTOMERS ||--o{ FACT_ORDERS : places
    DIM_RESTAURANTS ||--o{ FACT_ORDERS : receives
    DIM_DRIVERS ||--o{ FACT_ORDERS : delivers
    DIM_DATE ||--o{ FACT_ORDERS : occurs_on
    DIM_RESTAURANTS ||--o{ DIM_MENU_ITEMS : offers
    FACT_ORDERS ||--o{ FACT_ORDER_ITEMS : contains
    DIM_MENU_ITEMS ||--o{ FACT_ORDER_ITEMS : ordered
    DIM_DRIVERS ||--o{ FACT_DRIVER_LOCATIONS : tracked
```

## Data Pipeline Architecture

```mermaid
graph TD
    %% Data Sources
    A[Order System] --> B[Kafka: orders-topic]
    C[Driver App] --> D[Kafka: driver-locations-topic]
    E[Restaurant System] --> F[Kafka: menu-updates-topic]
    G[Batch Files] --> H[MinIO Raw Storage]
    
    %% Bronze Layer
    B --> I[Bronze: raw_orders]
    D --> J[Bronze: raw_driver_locations]
    F --> K[Bronze: raw_menu_items]
    H --> L[Bronze: raw_customers]
    H --> M[Bronze: raw_restaurants]
    
    %% Silver Layer Processing
    I --> N[Silver: cleaned_orders]
    J --> O[Silver: cleaned_driver_locations]
    K --> P[Silver: cleaned_menu_items]
    L --> Q[Silver: dim_customers]
    M --> R[Silver: dim_restaurants]
    
    %% Gold Layer Processing
    N --> S[Gold: fact_orders]
    O --> T[Gold: fact_driver_locations]
    P --> U[Gold: dim_menu_items]
    Q --> V[Gold: dim_customers_scd2]
    R --> W[Gold: dim_restaurants_scd2]
    
    %% Data Quality
    X[Data Quality Checks] --> N
    X --> O
    X --> P
    X --> Q
    X --> R
    
    %% Orchestration
    Y[Airflow DAGs] --> Z[Spark Jobs]
    Z --> I
    Z --> J
    Z --> K
    Z --> N
    Z --> O
    Z --> P
    Z --> S
    Z --> T
    Z --> U
```

## Slowly Changing Dimensions (SCD Type 2)

The following dimensions implement SCD Type 2 to track historical changes:
- **dim_customers**: Track customer profile changes
- **dim_restaurants**: Track restaurant information changes
- **dim_drivers**: Track driver status and rating changes

### SCD Type 2 Fields
- `scd_version`: Version number for each record
- `is_current`: Boolean flag indicating current record
- `created_at`: When the record was created
- `updated_at`: When the record was last updated 