{{
  config(
    materialized = 'incremental',
    unique_key = ['host_id'],
    strategy = 'merge'
    )
}}

/*
 Host Data intermediate table will maintain the host details. 
 Any changes to host data is set to SCD1 type implementation
 */

select
    distinct
    host_id,
    host_name,
    host_since_date,
    host_location,
    host_verification_methods_array
from {{ ref('stg_aws_s3__listings_data') }}