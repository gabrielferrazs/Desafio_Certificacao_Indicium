with 
    salesorderheader as (
        select *
        from {{ ref('stg_erp__salesorderheader') }} 
    )

,   salesorderdetail as (
        select *
        from {{ ref('stg_erp__salesorderdetail') }}
    )

,   joined as (
        select 
        salesorderheader.PK_SALES_ORDER || '-' || salesorderdetail.PK_SALES_ORDER_DETAIL as sk_sales
        , salesorderheader.PK_SALES_ORDER
        , salesorderdetail.PK_SALES_ORDER_DETAIL
        , salesorderheader.FK_CUSTOMER
        , salesorderheader.FK_SALES_PERSON
        , salesorderheader.FK_TERRITORY
        , salesorderdetail.FK_PRODUCT
        , salesorderheader.FK_CREDIT_CARD        
        --, salesorderdetail.FK_SPECIAL_OFFER        
        --, salesorderheader.FK_BILL_TO_ADDRESS
        --, salesorderheader.FK_SHIP_TO_ADDRESS
        --, salesorderheader.FK_SHIP_METHOD
        --, salesorderheader.FK_CURRENCY_RATE
        --, salesorderheader.REVISION_NUMBER
        , salesorderheader.ORDER_DATE
        , salesorderheader.DUE_DATE
        , salesorderheader.SHIP_DATE
        , salesorderheader.STATUS
        , salesorderheader.ONLINE_ORDER_FLAG
        --, salesorderheader.PURCHASE_ORDER_NUMBER
        --, salesorderheader.ACCOUNT_NUMBER
        --, salesorderheader.CREDIT_CARD_APPROVAL_CODE
        , salesorderheader.SUBTOTAL
        , salesorderheader.TAXA_MT
        , salesorderheader.FREIGHT
        , salesorderheader.TOTAL_DUE
        --, salesorderheader.COMMENT
        --, salesorderheader.ROWGUID
        --, salesorderheader.MODIFIED_DATE
        --, salesorderdetail.PK_SALES_ORDER
        --, salesorderdetail.CARRIER_TRACKING_NUMBER
        , salesorderdetail.ORDER_QTY
        , salesorderdetail.UNIT_PRICE
        , salesorderdetail.UNIT_PRICE_DISCOUNT
        --, salesorderdetail.ROWGUID
        , salesorderdetail.MODIFIED_DATE
        
        from salesorderheader
        left join salesorderdetail
            on salesorderheader.pk_sales_order = salesorderdetail.pk_sales_order
        
    )

select *
from joined
