with source as (
        select * from {{ source('erp', 'store') }}
  ),
  renamed as (
      select
        , cast(businessentityid as int) as pk_business_entity
        , cast(name as varchar) as name_store
        , cast(salespersonid as int) as fk_sales_person
        , cast(demographics as varchar) as demographics
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as timestamp) as modified_date

      from source
  )
  select * from renamed
    