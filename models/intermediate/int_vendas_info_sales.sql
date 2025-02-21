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
        select distinct
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
        select distinct
        SK_SALES
        , PK_SALES_ORDER
        , PK_SALES_ORDER_DETAIL
        , FK_CUSTOMER
        , FK_TERRITORY
        , FK_PRODUCT
        , FK_CREDIT_CARD
        , ORDER_DATE
        , STATUS
        , ONLINE_ORDER_FLAG
        , ORDER_QTY
        , UNIT_PRICE
        , UNIT_PRICE_DISCOUNT        
        , SUBTOTAL
        , TAXA_MT
        , FREIGHT
        , total_purchase
        , cast(UNIT_PRICE * ORDER_QTY as numeric(38,2)) as Valor_total_negociado
        , cast(UNIT_PRICE * ORDER_QTY * (1 - UNIT_PRICE_DISCOUNT) as numeric(38,2)) as Valor_total_liquido
        , cast(SUM(UNIT_PRICE * ORDER_QTY) 
                OVER () / COUNT(pk_sales_order) OVER () as numeric(38,2)) AS ticket_medio_bruto
        , cast(SUM(UNIT_PRICE * ORDER_QTY * (1 - UNIT_PRICE_DISCOUNT)) 
                OVER () / COUNT(pk_sales_order) OVER () as numeric(38,2)) AS ticket_medio_liquido
        from join_sales

    )
    
select 
*
from metricas
