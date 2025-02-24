with
   salesreason as (
        select * 
        from {{ ref('stg_erp__salesreason') }}
    )

,   salesorderheadersalesreason as (
        select * 
        from {{ ref('stg_erp__salesorderheadersalesreason') }}
    )

,   join_reason as (
        select
        salesreason.pk_sales_reason || '-' || coalesce(salesorderheadersalesreason.pk_sales_order  || ' ', '')  as sk_reason
        , salesorderheadersalesreason.pk_sales_order
        , salesreason.pk_sales_reason
        , salesreason.reason
        , salesreason.reason_type
        from salesreason
        left join salesorderheadersalesreason
            on salesreason.pk_sales_reason = salesorderheadersalesreason.fk_sales_reason
    
    )

select *
from join_reason    