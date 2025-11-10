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
