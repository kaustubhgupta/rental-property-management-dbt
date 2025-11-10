{{
  config(
    materialized = 'incremental',
    unique_key = ['listing_id', 'host_id'],
    strategy = 'merge'
    )
}}

select
    listing_id,
    host_id,
    host_name,
    host_since_date,
    host_location,
    host_verification_methods_array
from {{ ref('stg_aws_s3__listings_data') }}