with source as (
        select * from {{ source('erp', 'salesorderdetail') }}
  ),
  renamed as (
      select
        cast(salesorderid as int) as pk_sales_order
        , cast(salesorderdetailid as int) as pk_sales_order_detail
        , cast(carriertrackingnumber as varchar) as carrier_tracking_number
        , cast(orderqty as int) as order_qty
        , cast(productid as int) as fk_product
        , cast(specialofferid as int) as fk_special_offer
        , cast(unitprice as numeric(38,2)) as unit_price
        , cast(unitpricediscount as numeric(38,2)) as unit_price_discount
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    