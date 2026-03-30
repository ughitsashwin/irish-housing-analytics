with source as (
    select * from {{ source('dbt_dev', 'property_prices') }}
),

cleaned as (
    select
        to_date(month, 'YYYY MMMM') as price_date,
        year(to_date(month, 'YYYY MMMM'))  as price_year,
        month(to_date(month, 'YYYY MMMM')) as price_month,
        trim(property_type) as property_type,
        case
            when property_type like 'National%'  then 'National'
            when property_type like 'Dublin%'    then 'Dublin'
            when property_type like 'Cork%'      then 'Cork'
            when property_type like 'Galway%'    then 'Galway'
            when property_type like 'Limerick%'  then 'Limerick'
            when property_type like 'Waterford%' then 'Waterford'
            else 'Other'
        end as region,
        case
            when property_type like '%all residential%' then 'All Residential'
            when property_type like '%houses%'          then 'Houses'
            when property_type like '%apartments%'      then 'Apartments'
            else 'Other'
        end as property_category,
        try_cast(value as float) as price_index
    from source
    where try_cast(value as float) is not null
)

select * from cleaned
