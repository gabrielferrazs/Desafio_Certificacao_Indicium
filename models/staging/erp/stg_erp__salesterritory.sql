with source as (
        select * from {{ source('erp', 'salesterritory') }}
  ),
  renamed as (
      select
        cast(territoryid as int) as pk_territory
        , cast(name as varchar) as name_country
        , cast(countryregioncode as varchar) as country_code
        , cast("group" as varchar) as region
        , cast(salesytd as numeric(38,2)) as sales_ytd
        , cast(saleslastyear as numeric(38,2)) as sales_last_year
        , cast(costytd as numeric) as cost_ytd
        , cast(costlastyear as numeric) as cost_last_year
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    