with b1 as (
    select 
        avg(ss_list_price) as B1_LP,
        count(ss_list_price) as B1_CNT,
        count(distinct ss_list_price) as B1_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 0 and 5
      and (ss_list_price between 127 and 137
           or ss_coupon_amt between 6610 and 7610
           or ss_wholesale_cost between 27 and 47)
),

b2 as (
    select 
        avg(ss_list_price) as B2_LP,
        count(ss_list_price) as B2_CNT,
        count(distinct ss_list_price) as B2_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 6 and 10
      and (ss_list_price between 105 and 115
           or ss_coupon_amt between 6852 and 7852
           or ss_wholesale_cost between 75 and 95)
),

b3 as (
    select 
        avg(ss_list_price) as B3_LP,
        count(ss_list_price) as B3_CNT,
        count(distinct ss_list_price) as B3_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 11 and 15
      and (ss_list_price between 140 and 150
           or ss_coupon_amt between 11659 and 12659
           or ss_wholesale_cost between 28 and 48)
),

b4 as (
    select 
        avg(ss_list_price) as B4_LP,
        count(ss_list_price) as B4_CNT,
        count(distinct ss_list_price) as B4_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 16 and 20
      and (ss_list_price between 181 and 191
           or ss_coupon_amt between 5477 and 6477
           or ss_wholesale_cost between 13 and 33)
),

b5 as (
    select 
        avg(ss_list_price) as B5_LP,
        count(ss_list_price) as B5_CNT,
        count(distinct ss_list_price) as B5_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 21 and 25
      and (ss_list_price between 121 and 131
           or ss_coupon_amt between 9051 and 10051
           or ss_wholesale_cost between 12 and 32)
),

b6 as (
    select 
        avg(ss_list_price) as B6_LP,
        count(ss_list_price) as B6_CNT,
        count(distinct ss_list_price) as B6_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 26 and 30
      and (ss_list_price between 186 and 196
           or ss_coupon_amt between 15083 and 16083
           or ss_wholesale_cost between 11 and 31)
),

b7 as (
    select 
        avg(ss_list_price) as B7_LP,
        count(ss_list_price) as B7_CNT,
        count(distinct ss_list_price) as B7_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 31 and 35
      and (ss_list_price between 100 and 110
           or ss_coupon_amt between 5000 and 6000
           or ss_wholesale_cost between 50 and 70)
),

b8 as (
    select 
        avg(ss_list_price) as B8_LP,
        count(ss_list_price) as B8_CNT,
        count(distinct ss_list_price) as B8_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 36 and 40
      and (ss_list_price between 130 and 140
           or ss_coupon_amt between 7000 and 8000
           or ss_wholesale_cost between 20 and 40)
),

b9 as (
    select 
        avg(ss_list_price) as B9_LP,
        count(ss_list_price) as B9_CNT,
        count(distinct ss_list_price) as B9_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 41 and 45
      and (ss_list_price between 150 and 160
           or ss_coupon_amt between 11000 and 12000
           or ss_wholesale_cost between 30 and 50)
),

b10 as (
    select 
        avg(ss_list_price) as B10_LP,
        count(ss_list_price) as B10_CNT,
        count(distinct ss_list_price) as B10_CNTD
    from {{ ref('store_sales') }}
    where ss_quantity between 46 and 50
      and (ss_list_price between 200 and 210
           or ss_coupon_amt between 16000 and 17000
           or ss_wholesale_cost between 40 and 60)
)

-- Final model combining all the aggregates
select * from b1
union all
select * from b2
union all
select * from b3
union all
select * from b4
union all
select * from b5
union all
select * from b6
union all
select * from b7
union all
select * from b8
union all
select * from b9
union all
select * from b10