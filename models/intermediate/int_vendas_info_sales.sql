with 
    salesorderheader as (
        select *
        from {{ ref('stg_erp__salesorderheader') }} 
    )

,   salesorderdetail as (
        select *
        from {{ ref('stg_erp__salesorderdetail') }}
    )

,   join_sales as (
        select
            salesorderheader.pk_sales_order || '-' || salesorderdetail.pk_sales_order_detail as sk_sales
            , salesorderheader.pk_sales_order
            , salesorderdetail.pk_sales_order_detail
            , salesorderheader.fk_customer
            , salesorderheader.fk_territory
            , salesorderdetail.fk_product
            , salesorderheader.fk_credit_card        
            , salesorderheader.order_date
            , salesorderheader.status
            , salesorderheader.online_order_flag
            , salesorderheader.subtotal
            , salesorderheader.taxa_mt
            , salesorderheader.freight
            , salesorderheader.total_due as total_purchase
            , salesorderdetail.order_qty
            , salesorderdetail.unit_price
            , salesorderdetail.unit_price_discount      
        from salesorderheader
        left join salesorderdetail
            on salesorderheader.pk_sales_order = salesorderdetail.pk_sales_order
    )
    
,   metricas as (
        select 
            sk_sales
            , pk_sales_order as fk_sales_order
            , pk_sales_order_detail as fk_sales_order_detail
            , fk_customer
            , fk_territory
            , fk_product
            , fk_credit_card
            , order_date as fk_date
            , status
            , online_order_flag
            , order_qty
            , unit_price
            , unit_price_discount        
            --, subtotal
            --, taxa_mt
            , freight
            --, total_purchase
            , cast(unit_price * order_qty as numeric(38,4)) as valor_total_negociado
            , cast(unit_price * order_qty * (1 - unit_price_discount) as numeric(38,2)) as valor_total_liquido
        from join_sales
        
    )
    
select 
*
from metricas
