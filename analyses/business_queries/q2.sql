{# Write a query to find the average price increase for each neighborhood from July 12th
2021 to July 11th 2022. #}

with 

listings_prices_in_july_2021 as (
  select
    listing_id,
    listing_in_point_price_usd as price_2021,
  from {{ ref('listings_daily_report') }}
  where calendar_date <= '2021-07-12'
  qualify row_number() over (partition by listing_id order by calendar_date desc) =1

),

listings_prices_in_july_2022 as (
  select
    listing_id,
    listing_in_point_price_usd as price_2022,
  from {{ ref('listings_daily_report') }}
  where calendar_date <= '2022-07-11'
  qualify row_number() over (partition by listing_id order by calendar_date desc) =1

),

listings_neighborhood as (
  select distinct listing_id, listing_neighborhood
  from {{ ref('listings_daily_report') }}
),

per_listing_diff as (
  select
    l.listing_id,
    l.listing_neighborhood,
    p21.price_2021,
    p22.price_2022,
    abs(coalesce(p22.price_2022,0) - coalesce(p21.price_2021,0)) as price_diff
  from listings_neighborhood l
  left join listings_prices_in_july_2021 p21 on l.listing_id = p21.listing_id
  left join listings_prices_in_july_2022 p22 on l.listing_id = p22.listing_id
  where p21.price_2021 is not null or p22.price_2022 is not null
)


select
  listing_neighborhood, 
  round(avg(price_diff), 2) as avg_price_increase_usd,
  count(*) as listings_count
from per_listing_diff
group by listing_neighborhood
order by avg_price_increase_usd desc;
