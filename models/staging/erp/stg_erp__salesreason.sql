with source as (
        select * from {{ source('erp', 'salesreason') }}
  ),
  renamed as (
      select
        cast(salesreasonid as int) as pk_sales_reason
        , cast(name as varchar) as reason
        , cast(reasontype as varchar) as reason_type
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    