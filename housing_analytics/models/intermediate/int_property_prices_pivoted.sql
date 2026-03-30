-- INTERMEDIATE LAYER
-- Purpose: Apply business logic and prepare data for the final mart.
-- This model aggregates the cleaned staging data to a useful grain.
-- "Grain" means the level of detail — here each row is one region,
-- one property category, for one month.

with staging as (
    -- Reference staging model using ref() macro
    -- ref() is how dbt models talk to each other — it builds the dependency
    -- graph automatically so dbt knows to run stg_ before int_
    select * from {{ ref('stg_property_prices') }}
),

aggregated as (
    select
        price_date,
        price_year,
        price_month,
        region,
        property_category,
        
        -- Average index value for this region/category/month combination
        round(avg(price_index), 2) as avg_price_index,
        
        -- Count of data points going into this average
        count(*) as data_points

    from staging
    -- Exclude catch-all categories to keep the data clean
    where region != 'Other'
      and property_category != 'Other'
    group by 1, 2, 3, 4, 5
),

-- Add month-over-month change using window functions
-- lag() looks at the previous row in the partition
-- This is an important SQL technique worth knowing for interviews
with_changes as (
    select
        *,
        lag(avg_price_index) over (
            partition by region, property_category
            order by price_date
        ) as prev_month_index,

        round(
            avg_price_index - lag(avg_price_index) over (
                partition by region, property_category
                order by price_date
            ),
        2) as mom_change,  -- month over month absolute change

        round(
            (avg_price_index - lag(avg_price_index) over (
                partition by region, property_category
                order by price_date
            )) / nullif(lag(avg_price_index) over (
                partition by region, property_category
                order by price_date
            ), 0) * 100,
        2) as mom_pct_change  -- month over month percentage change

    from aggregated
)

select * from with_changes