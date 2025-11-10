with 

listing_with_lockbox as (
{{generate_targetted_array_element_distinct_values('listings_daily_report', 'amenities_available_array', 'lockbox', 'listing_id')}}
), 

listing_with_first_aid as (
{{generate_targetted_array_element_distinct_values('listings_daily_report', 'amenities_available_array', 'first aid kit', 'listing_id')}}
), 


listing_date_data as (
select
listing_id, 
calendar_date, 
minimum_nights_to_book, 
maximum_nights_to_book, lag(calendar_date) over(partition by listing_id order by calendar_date asc) previous_calendar_day
from {{ ref('listings_daily_report') }}
inner join listing_with_lockbox
using(listing_id)
inner join listing_with_first_aid
using(listing_id)
where is_available 
order by listing_id, calendar_date asc)

,flag_data as
  (SELECT
    listing_id,
    calendar_date,
    CASE
      WHEN previous_calendar_day IS NULL THEN 1                   
      WHEN datediff('day', previous_calendar_day, calendar_date) = 1 THEN 0  
      ELSE 1                                          
    END AS reverse_continue
  FROM listing_date_data)

, num_days_data as (
  SELECT
    listing_id,
    calendar_date,
    SUM(reverse_continue) OVER (PARTITION BY listing_id ORDER BY calendar_date ROWS UNBOUNDED PRECEDING) + 1 AS num_days
  FROM flag_data
  )
, listing_available_stay_durations as (
SELECT
  listing_id,
  MIN(calendar_date) AS start_date,
  MAX(calendar_date) AS end_date,
  COUNT(*)           AS num_days_available
FROM num_days_data
GROUP BY listing_id, num_days
ORDER BY listing_id, start_date)

select a.*, b.minimum_nights_to_book, b.maximum_nights_to_book
from listing_available_stay_durations a
left join (select distinct listing_id, minimum_nights_to_book, maximum_nights_to_book from listing_date_data) b
using(listing_id)
where num_days_available between b.minimum_nights_to_book and b.maximum_nights_to_book
qualify max(num_days_available) over() = num_days_available