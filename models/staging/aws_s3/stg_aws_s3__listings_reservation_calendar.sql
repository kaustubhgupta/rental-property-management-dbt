with source as (
        select * from {{ source('aws_s3', 'listings_reservation_calendar') }}
  ),
  renamed as (
      select
        {{ adapter.quote("LISTING_ID") }} AS listing_id,
        {{ adapter.quote("DATE") }}::date AS calendar_date,  -- These castings can be encapsulated into a dispatch function for multiwarehouse support
        CASE 
          WHEN {{ adapter.quote("AVAILABLE") }} = 'f' THEN False ELSE True 
        END:: boolean AS is_available,  -- Boolean handling
        NULLIF(LOWER({{ adapter.quote("RESERVATION_ID") }}), 'null')::integer AS reservation_id,  -- NULL handling for NULL as string
        {{ adapter.quote("PRICE") }}::float AS listing_in_point_price_usd,
        {{ adapter.quote("MINIMUM_NIGHTS") }} AS minimum_nights_to_book,
        {{ adapter.quote("MAXIMUM_NIGHTS") }} AS maximum_nights_to_book

      from source
  )
  select * from renamed    