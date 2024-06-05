with date_range_cte as (
  select
    d_date_sk
  from {{ ref('date_dim') }}
  where d_date between cast('2001-08-21' as date) and cast('2001-09-20' as date)
),
store_sales_cte as (
  select
    ss.ss_store_sk,
    sum(ss.ss_ext_sales_price) as sales,
    sum(ss.ss_net_profit) as profit
  from {{ ref('store_sales') }} ss
  where ss.ss_sold_date_sk in (select d_date_sk from date_range_cte)
  group by ss.ss_store_sk
),
store_returns_cte as (
  select
    sr.sr_store_sk,
    sum(sr.sr_return_amt) as returns,
    sum(sr.sr_net_loss) as profit_loss
  from {{ ref('store_returns') }} sr
  where sr.sr_returned_date_sk in (select d_date_sk from date_range_cte)
  group by sr.sr_store_sk
),
catalog_sales_cte as (
  select
    cs.cs_call_center_sk,
    sum(cs.cs_ext_sales_price) as sales,
    sum(cs.cs_net_profit) as profit
  from {{ ref('catalog_sales') }} cs
  where cs.cs_sold_date_sk in (select d_date_sk from date_range_cte)
  group by cs.cs_call_center_sk
),
catalog_returns_cte as (
  select
    cr.cr_call_center_sk,
    sum(cr.cr_return_amount) as returns,
    sum(cr.cr_net_loss) as profit_loss
  from {{ ref('catalog_returns') }} cr
  where cr.cr_returned_date_sk in (select d_date_sk from date_range_cte)
  group by cr.cr_call_center_sk
),
web_sales_cte as (
  select
    ws.ws_web_page_sk,
    sum(ws.ws_ext_sales_price) as sales,
    sum(ws.ws_net_profit) as profit
  from {{ ref('web_sales') }} ws
  where ws.ws_sold_date_sk in (select d_date_sk from date_range_cte)
  group by ws.ws_web_page_sk
),
web_returns_cte as (
  select
    wr.wr_web_page_sk,
    sum(wr.wr_return_amt) as returns,
    sum(wr.wr_net_loss) as profit_loss
  from {{ ref('web_returns') }} wr
  where wr.wr_returned_date_sk in (select d_date_sk from date_range_cte)
  group by wr.wr_web_page_sk
),
store_final_cte as (
  select
    'store channel' as channel,
    ss.ss_store_sk as id,
    ss.sales,
    coalesce(sr.returns, 0) as returns,
    (ss.profit - coalesce(sr.profit_loss, 0)) as profit
  from store_sales_cte ss
  left join store_returns_cte sr on ss.ss_store_sk = sr.sr_store_sk
),
catalog_final_cte as (
  select
    'catalog channel' as channel,
    cs.cs_call_center_sk as id,
    cs.sales,
    coalesce(cr.returns, 0) as returns,
    (cs.profit - coalesce(cr.profit_loss, 0)) as profit
  from catalog_sales_cte cs
  left join catalog_returns_cte cr on cs.cs_call_center_sk = cr.cr_call_center_sk
),
web_final_cte as (
  select
    'web channel' as channel,
    ws.ws_web_page_sk as id,
    ws.sales,
    coalesce(wr.returns, 0) as returns,
    (ws.profit - coalesce(wr.profit_loss, 0)) as profit
  from web_sales_cte ws
  left join web_returns_cte wr on ws.ws_web_page_sk = wr.wr_web_page_sk
)
select
  channel,
  id,
  sum(sales) as sales,
  sum(returns) as returns,
  sum(profit) as profit
from (
  select * from store_final_cte
  union all
  select * from catalog_final_cte
  union all
  select * from web_final_cte
) x
group by rollup(channel, id)
order by channel, id
limit 100