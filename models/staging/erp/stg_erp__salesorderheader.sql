with source as (
        select * from {{ source('erp', 'salesorderheader') }}
  ),
  renamed as (
      select
        cast(salesorderid as int) as pk_sales_order
        , cast(revisionnumber as int) as revision_number
        , cast(orderdate as timestamp) as order_date
        , cast(duedate as timestamp) as due_date
        , cast(shipdate as timestamp) as ship_date
        , cast(status as int) as status
        , cast(onlineorderflag as boolean) as online_order_flag
        , cast(purchaseordernumber as varchar) as purchase_order_number
        , cast(accountnumber as varchar) as account_number
        , cast(customerid as int) as fk_customer
        , cast(salespersonid as int) as fk_sales_person
        , cast(territoryid as int) as fk_territory
        , cast(billtoaddressid as int) as fk_bill_to_address
        , cast(shiptoaddressid as int) as fk_ship_to_address
        , cast(shipmethodid as int) as fk_ship_method
        , cast(creditcardid as int) as fk_credit_card
        , cast(creditcardapprovalcode as varchar) as credit_card_approval_code
        , cast(currencyrateid as int) as fk_currency_rate
        , cast(subtotal as numeric(38,2)) as subtotal
        , cast(taxamt as numeric(38,2)) as taxa_mt
        , cast(freight as numeric(38,2)) as freight
        , cast(totaldue as numeric(38,2)) as total_due
        , cast(comment as varchar) as comment
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    