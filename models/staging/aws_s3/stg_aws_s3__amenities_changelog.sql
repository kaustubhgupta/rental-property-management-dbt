with source as (
        select * from {{ source('aws_s3', 'amenities_changelog') }}
  ),
  renamed as (
      select
          {{ adapter.quote("LISTING_ID") }} AS listing_id,
        {{ adapter.quote("CHANGE_AT") }} AS change_at_date,
        {{ adapter.quote("AMENITIES") }} AS amenities_available_array

      from source
  )
  select * from renamed
    