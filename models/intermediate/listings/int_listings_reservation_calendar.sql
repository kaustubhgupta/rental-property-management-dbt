{{
  config(
    materialized = 'incremental',
    unique_key = ['listing_id', 'calendar_date'],
    strategy = 'merge'
    )
}}

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
-- append new calendar rows after the latest date in the existing model
where calendar_date > (select max(calendar_date) from {{ this }}) 
      or listing_id not in (select distinct listing_id from {{ this }})
{% endif %}
