with source as (
        select * from {{ source('erp', 'salesorderheadersalesreason') }}
  ),
  renamed as (
      select
        cast(salesorderid as int) as pk_sales_order
        , cast(salesreasonid as int) as fk_sales_reason
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    