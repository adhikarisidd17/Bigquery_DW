--View 
with customers as (
  SELECT 
    date_diff(current_date(),customer_dob,YEAR) age, --customer age
    case ceil(safe_divide(date_diff(current_date(),customer_dob,YEAR),10))
  when 5 then '41-50'
  when 6 then '51-60'
  when 7 then '61-70'
  when 8 then '71-80'
  when 9 then '81-90'
  when 10 then '91-100' end as age_bin,
    * 
  FROM `normative-analytics.dbt_sidd.test_customer_data` 
),

tansaction_details as (
  select 
    *,
    format_date('%A',transaction_date) weekday 
  from `dbt_sidd.test_transaction_data`
),
--Transactions aggregate at customer level
transactions as (
  select 
    customer_id, 
    sum(sales_amount) sales_amount
  from tansaction_details
  group by 1
),

customer_visits as (
 with visits as (
  select 
    customer_id,
    weekday,
    count(distinct store_id) no_visits --Customer can visit many stores on the same day.
  from tansaction_details
  group by 1,2
  )
  select * from visits
  pivot (sum(no_visits) for weekday in ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday') )
),

offers as (
  select  
    farm_fingerprint(concat(customer_id,offer_id)) as id, --unique id
    * ,
  from `dbt_sidd.test_offer_data`
),

received_offers as (
  select 
    customer_id, 
    count(distinct offer_id) offers_received 
  from offers group by 1
),

offers_per_customer as (
  select 
    f.customer_id,
    count(distinct case when d.id is not null 
    and f.transaction_date between d.start_date and d.end_date 
    then f.offer_id end) as offers_redeemed, -- If the transaction falls within the offer validity date and if the offer exist for the customer then it is considered redeemed.
  from tansaction_details f 
  left join offers d on d.customer_id = f.customer_id and d.offer_id = f.offer_id
  where true 
  group by 1
)
--Put everything together
select 
  t.customer_id, 
  c.customer_name, 
  c.age,
  c.age_bin,
  ro.offers_received,
  o.offers_redeemed,
  t.sales_amount,
  pcv.* except(customer_id)
from transactions t
left join customers c on t.customer_id = c.customer_id
left join customer_visits pcv on pcv.customer_id = t.customer_id
left join offers_per_customer o on o.customer_id = t.customer_id
left join received_offers ro on ro.customer_id = t.customer_id
;
