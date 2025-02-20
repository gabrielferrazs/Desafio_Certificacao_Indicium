with source as (
        select * from {{ source('erp', 'productcategory') }}
  ),
  renamed as (
      select
        cast(productcategoryid as int) as pk_product_category
        , cast(name as varchar) as name_category
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
