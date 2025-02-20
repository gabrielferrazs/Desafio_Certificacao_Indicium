with source as (
        select * from {{ source('erp', 'customer') }}
  ),
  renamed as (
      select
        cast(customerid as int) as pk_customer
        , cast(personid as int) as fk_person
        , cast(storeid as int) as fk_store
        , cast(territoryid as int) as fk_territory
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as timestamp) as modified_date

      from source
  )
  select * from renamed
    