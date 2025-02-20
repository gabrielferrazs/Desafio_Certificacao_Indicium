with source as (
        select * from {{ source('erp', 'countryregion') }}
  ),
  renamed as (
      select
        cast(countryregioncode as varchar) as pk_country_region_code
        , cast(name as varchar) as name_country
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    