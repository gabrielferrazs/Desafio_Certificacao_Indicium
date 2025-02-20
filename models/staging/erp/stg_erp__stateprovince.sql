with source as (
        select * from {{ source('erp', 'stateprovince') }}
  ),
  renamed as (
      select
        cast(stateprovinceid as int) as pk_state_province
        , cast(territoryid as int) as fk_territory
        , cast(countryregioncode as varchar) as fk_country_region_code        
        , cast(stateprovincecode as varchar) as state_province_code
        , cast(isonlystateprovinceflag as boolean) as is_only_state_province_flag
        , cast(name as varchar) as name_state
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as timestamp) as modified_date

      from source
  )
  select * from renamed
    