{{
  config(
    materialized = 'view',
    )
}}

/* 
  Listing latest price view.
  This view will return latest price of listing as per latest calendar date
 */

select listing_id, listing_in_point_price_usd listing_latest_price_usd
from {{ ref('int_listings_reservation_calendar') }} 
qualify max(calendar_date) over(partition by listing_id) = calendar_date