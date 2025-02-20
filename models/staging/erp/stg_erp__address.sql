with source as (
        select * from {{ source('erp', 'address') }}
  ),
  renamed as (
      select
        cast(addressid as int) as pk_address
        , cast(stateprovinceid as int) as fk_state_province
        , cast(addressline1 as varchar) as addressline1
        , cast(addressline2 as varchar) as addressline2
        , cast(city as varchar) as name_city
        , cast(postalcode as varchar) as postal_code
        , cast(spatiallocation as varchar) as spatial_location
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    