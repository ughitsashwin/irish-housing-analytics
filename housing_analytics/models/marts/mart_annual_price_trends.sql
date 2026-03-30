-- MARTS LAYER
-- Purpose: Final business-ready table optimised for analysis and reporting.
-- This is what a BI tool, dashboard, or analyst would query directly.
-- We aggregate to annual level here for clean year-on-year comparisons.

with intermediate as (
    select * from {{ ref('int_property_prices_pivoted') }}
),

annual as (
    select
        price_year,
        region,
        property_category,

        -- Annual averages
        round(avg(avg_price_index), 2)     as annual_avg_index,
        round(min(avg_price_index), 2)     as annual_min_index,
        round(max(avg_price_index), 2)     as annual_max_index,

        -- Total data points for the year
        sum(data_points)                   as total_data_points,

        -- How many months of data we have (should be 12 for complete years)
        count(distinct price_month)        as months_of_data

    from intermediate
    group by 1, 2, 3
),

-- Add year-over-year growth using the same lag() technique
with_yoy as (
    select
        *,
        lag(annual_avg_index) over (
            partition by region, property_category
            order by price_year
        ) as prev_year_index,

        round(
            (annual_avg_index - lag(annual_avg_index) over (
                partition by region, property_category
                order by price_year
            )) / nullif(lag(annual_avg_index) over (
                partition by region, property_category
                order by price_year
            ), 0) * 100,
        2) as yoy_growth_pct  -- year over year percentage growth

    from annual
)

select * from with_yoy
-- Only include years with a full 12 months of data for accuracy
where months_of_data = 12
order by price_year, region, property_category