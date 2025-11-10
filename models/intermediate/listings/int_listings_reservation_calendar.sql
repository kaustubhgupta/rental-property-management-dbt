{{
  config(
    materialized = 'incremental',
    unique_key = ['listing_id', 'calendar_date'],
    strategy = 'merge'
    )
}}

/*
  Listings reservation intermediate table at calendar day level.
  Price is as per the calendar day and can vary for listings
 */

select
    listing_id,
    calendar_date,
    is_available,
    reservation_id,
    listing_in_point_price_usd,
    minimum_nights_to_book,
    maximum_nights_to_book
from {{ ref('stg_aws_s3__listings_reservation_calendar') }}

{% if is_incremental() %}
-- Fetching either new calendar dates data or loading new listings old data
where calendar_date > (select max(calendar_date) from {{ this }}) 
      or listing_id not in (select distinct listing_id from {{ this }})
{% endif %}
