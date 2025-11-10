{# Write a query to find the total revenue and percentage of revenue by month segmented
by whether or not air conditioning exists on the listing. #}

with 

listing_with_ac as (
{{generate_targetted_array_element_distinct_values('listings_daily_report', 'amenities_available_array', 'air conditioning', 'listing_id')}}

), 

listing_revenue_per_month as (
  select
    listing_id,
    TO_CHAR(calendar_date, 'YYYY_MM') year_month,
    sum(listing_in_point_price_usd) total_revenue_per_listing_per_month
  from {{ ref('listings_daily_report') }}
  where is_available=False
  group by all
  order by 2,1
)
select year_month
,sum(total_revenue_per_listing_per_month) total_revenue_per_month
,round((sum(case when b.listing_id is null then total_revenue_per_listing_per_month else 0 end)*100.0)/(sum(total_revenue_per_listing_per_month)),2) as percent_revenue_per_month_without_ac
,round((sum(case when b.listing_id is not null then total_revenue_per_listing_per_month else 0 end)*100.0)/(sum(total_revenue_per_listing_per_month)),2) as percent_revenue_per_month_with_ac
from listing_revenue_per_month a
left join listing_with_ac b
using(listing_id)
group by all
order by 1 asc
