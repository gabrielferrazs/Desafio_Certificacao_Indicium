with source as (
        select * from {{ source('erp', 'productsubcategory') }}
  ),
  renamed as (
      select
        cast(productsubcategoryid as int) as pk_product_subcategory
        , cast(productcategoryid as int) as fk_product_category
        , cast(name as varchar) as name_sub_category
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    