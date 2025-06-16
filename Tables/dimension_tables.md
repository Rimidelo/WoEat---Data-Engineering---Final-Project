erDiagram
    FACT_ORDERS {
        bigint order_key PK
        string order_id
        bigint customer_key FK
        bigint restaurant_key FK
        bigint driver_key FK
        string date_key FK
        timestamp order_time
        timestamp pickup_time
        timestamp delivery_time
        double total_amount
        double delivery_fee
        double tip_amount
        int prep_minutes
        int delivery_minutes
        double distance_km
        boolean cancelled
        boolean sla_breach
        string order_status
        string payment_method
        timestamp ingest_timestamp
    }
    
    DIM_DRIVERS {
        bigint driver_key PK
        string driver_id
        string name
        string phone
        string license_number
        string vehicle_type
        string vehicle_model
        string zone
        double rating
        string status
        date record_start_date
        date record_end_date
        boolean is_current
        int scd_version
        timestamp ingest_timestamp
    }
    
    DIM_RESTAURANTS {
        bigint restaurant_key PK
        string restaurant_id
        string restaurant_name
        string cuisine_type
        string address
        string city
        string zone
        string phone
        double rating
        int avg_prep_time
        double delivery_fee
        string status
        date record_start_date
        date record_end_date
        boolean is_current
        int scd_version
        timestamp ingest_timestamp
    }
    
    DIM_DATE {
        string date_key PK
        date full_date
        int year
        int month
        int day
        int quarter
        string day_name
        string month_name
        boolean is_weekend
        boolean is_holiday
        int fiscal_year
        int fiscal_quarter
    }
    
    FACT_ORDER_ITEMS {
        bigint item_key PK
        bigint order_key FK
        bigint menu_item_key FK
        int quantity
        double unit_price
        double total_price
        string special_instructions
        timestamp ingest_timestamp
    }
    
    DIM_MENU_ITEMS {
        bigint menu_item_key PK
        string item_id
        bigint restaurant_key FK
        string item_name
        string category
        double price
        string description
        boolean is_available
        timestamp ingest_timestamp
    }
    
    FACT_RATINGS {
        bigint rating_key PK
        bigint order_key FK
        bigint driver_key FK
        bigint restaurant_key FK
        string date_key FK
        double food_rating
        double delivery_rating
        double driver_rating
        double overall_rating
        string comments
        timestamp rating_time
        timestamp ingest_timestamp
    }
    
    FACT_BUSINESS_SUMMARY {
        bigint summary_key PK
        string date_key FK
        bigint total_orders
        double total_revenue
        double avg_order_value
        double avg_delivery_time
        double sla_breach_rate
        bigint total_drivers_active
        bigint total_restaurants_active
        double overall_satisfaction
        timestamp ingest_timestamp
    }
    
    %% Relationships
    DIM_DRIVERS ||--o{ FACT_ORDERS : delivers
    DIM_RESTAURANTS ||--o{ FACT_ORDERS : prepares
    DIM_DATE ||--o{ FACT_ORDERS : occurs_on
    FACT_ORDERS ||--o{ FACT_ORDER_ITEMS : contains
    DIM_MENU_ITEMS ||--o{ FACT_ORDER_ITEMS : ordered_item
    DIM_RESTAURANTS ||--o{ DIM_MENU_ITEMS : offers
    FACT_ORDERS ||--o{ FACT_RATINGS : has_rating
    DIM_DRIVERS ||--o{ FACT_RATINGS : rated_driver
    DIM_RESTAURANTS ||--o{ FACT_RATINGS : rated_restaurant
    DIM_DATE ||--o{ FACT_RATINGS : rated_on
    DIM_DATE ||--o{ FACT_BUSINESS_SUMMARY : summarized_on