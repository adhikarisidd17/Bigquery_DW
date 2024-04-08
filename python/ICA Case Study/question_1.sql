with customers as (
  SELECT 
  date_diff(current_date(),customer_dob,YEAR) age, --customer age
  case ceil(safe_divide(date_diff(current_date(),customer_dob,YEAR),10))
  when 5 then '41-50'
  when 6 then '51-60'
  when 7 then '61-70'
  when 8 then '71-80'
  when 9 then '81-90'
  when 10 then '91-100' end as age_bin, #binning
  * 
  FROM `normative-analytics.dbt_sidd.test_customer_data` 
),

unique_offers as (
# Assumption: Customer can also redeem in-store offer 
  select distinct offer_id, start_date,end_date from `dbt_sidd.test_offer_data` 
  union all 
  select 0 as offer_id, date('1900-01-01')start_date , current_date() end_date #For No offer
),

tansaction_details as (
  select 
    t.*,
    format_date('%A',transaction_date) weekday,
    uo.offer_id is not null and t.transaction_date between uo.start_date and uo.end_date as is_valid_transaction 
  from `dbt_sidd.test_transaction_data` t
  left join unique_offers uo on uo.offer_id = ifnull(t.offer_id,0)
),

transactions_deduped as (
  select * from tansaction_details td where is_valid_transaction
  qualify row_number() over (partition by td.transaction_id order by td.store_id,td.product_id,td.customer_id,transaction_date)=1 #Assumption: Only one offer can be applied to a product at a time. 
),

--Transactions aggregated at customer level
transactions as (
  select 
    customer_id, 
    sum(sales_amount) sales_amount
  from transactions_deduped -- Remove all transactions with invalid offers.
  group by 1
),

--Visits
customer_visits as (
 with visits as (
  select 
    customer_id,
    weekday,
    count(distinct store_id) no_visits 
  from transactions_deduped
  group by 1,2
  )
  select * from visits
  pivot (sum(no_visits) for weekday in ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday') )
),

received_offers as (
  select 
    customer_id, 
    count(distinct offer_id) offers_received 
  from `dbt_sidd.test_offer_data` group by 1
),

customer_redeemed_offers as (
  select 
    f.customer_id,
    count(distinct f.offer_id) as offers_redeemed, 
  from transactions_deduped f
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
left join customer_redeemed_offers o on o.customer_id = t.customer_id
left join received_offers ro on ro.customer_id = t.customer_id
;
