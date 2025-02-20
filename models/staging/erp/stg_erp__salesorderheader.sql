with source as (
        select * from {{ source('erp', 'salesorderheader') }}
  ),
  renamed as (
      select
        cast(SALESORDERID as int) as pk_SALES_ORDER
        , cast(REVISIONNUMBER as int) as REVISION_NUMBER
        , cast(ORDERDATE as timestamp) as ORDER_DATE
        , cast(DUEDATE as timestamp) as DUE_DATE
        , cast(SHIPDATE as timestamp) as SHIP_DATE
        , cast(STATUS as int) as STATUS
        , cast(ONLINEORDERFLAG as boolean) as ONLINE_ORDER_FLAG
        , cast(PURCHASEORDERNUMBER as varchar) as PURCHASE_ORDER_NUMBER
        , cast(ACCOUNTNUMBER as varchar) as ACCOUNT_NUMBER
        , cast(CUSTOMERID as int) as fk_CUSTOMER
        , cast(SALESPERSONID as int) as fk_SALES_PERSON
        , cast(TERRITORYID as int) as fk_TERRITORY
        , cast(BILLTOADDRESSID as int) as fk_BILL_TO_ADDRESS
        , cast(SHIPTOADDRESSID as int) as fk_SHIP_TO_ADDRESS
        , cast(SHIPMETHODID as int) as fk_SHIP_METHOD
        , cast(CREDITCARDID as int) as fk_CREDIT_CARD
        , cast(CREDITCARDAPPROVALCODE as varchar) as CREDIT_CARD_APPROVAL_CODE
        , cast(CURRENCYRATEID as int) as fk_CURRENCY_RATE
        , cast(SUBTOTAL as numeric(38,2)) as SUBTOTAL
        , cast(TAXAMT as numeric(38,2)) as TAXA_MT
        , cast(FREIGHT as numeric(38,2)) as FREIGHT
        , cast(TOTALDUE as numeric(38,2)) as TOTAL_DUE
        , cast(COMMENT as varchar) as COMMENT
        , cast(ROWGUID as varchar) as ROWGUID
        , cast(MODIFIEDDATE as date) as MODIFIED_DATE

      from source
  )
  select * from renamed
    