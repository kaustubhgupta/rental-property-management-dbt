{{
  config(
    materialized = 'incremental',
    unique_key = ['review_id'],
    strategy= 'merge'
    )
}}

/*
  Listings review intermediate table
 */

select
    review_id,
    listing_id,
    listing_review_score,
    listing_review_date
from {{ ref('stg_aws_s3__listings_reviews') }}

{% if is_incremental() %}
-- append new review rows for unique review ids
where review_id not in (select distinct review_id from {{ this }} )
{% endif %}
