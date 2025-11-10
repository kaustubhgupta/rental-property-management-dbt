{{
  config(
    materialized = 'view',
    )
}}

select listing_id, listing_in_point_price_usd listing_latest_price_usd
from {{ ref('int_listings_reservation_calendar') }} 
qualify max(calendar_date) over(partition by listing_id) = calendar_date