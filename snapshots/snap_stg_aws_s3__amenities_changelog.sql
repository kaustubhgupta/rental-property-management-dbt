{% snapshot snap_stg_aws_s3__amenities_changelog %}

{{
  config(
    target_schema='snapshots',
    unique_key='listing_id',
    strategy='timestamp',
    updated_at='change_at_date'
  )
}}

select
    listing_id,
    change_at_date,
    amenities_available_array
from {{ ref('stg_aws_s3__amenities_changelog') }}
-- One time ingestion
{# from (
  select
    a.listing_id,
    a.change_at change_at_date,
    a.amenities amenities_available_array
from {{ source('aws_s3', 'amenities_changelog') }} a
join (
    select
        listing_id,
        min(change_at) as min_change_at
    from {{ source('aws_s3', 'amenities_changelog') }}
    group by listing_id
) b
on a.listing_id = b.listing_id
and a.change_at = b.min_change_at

) #}

{% endsnapshot %}
