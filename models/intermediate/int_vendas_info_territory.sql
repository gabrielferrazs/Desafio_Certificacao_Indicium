with 
    salesterritory as (
        select *
        from {{ ref('stg_erp__salesterritory') }}
    )

,   stateprovince as (
        select *
        from {{ ref('stg_erp__stateprovince') }}
    )

,   address as (
        select *
        from {{ ref('stg_erp__address') }}
    )   

,   countryregion as (
        select *
        from {{ ref('stg_erp__countryregion') }}
    ) 

,   joined as (
        select
        salesterritory.PK_TERRITORY
        , address.ADDRESSLINE1		
        , address.ADDRESSLINE2
        , address.NAME_CITY
        , address.POSTAL_CODE
        , stateprovince.NAME_STATE
        , stateprovince.STATE_PROVINCE_CODE
        , countryregion.name_country
        , salesterritory.COUNTRY_CODE
        , salesterritory.REGION
        --, salesterritory.SALES_YTD
        --, salesterritory.SALES_LAST_YEAR
        --, salesterritory.COST_YTD
        --, salesterritory.COST_LAST_YEAR
        --, stateprovince.PK_STATE_PROVINCE
        --, stateprovince.IS_ONLY_STATE_PROVINCE_FLAG
        --, address.PK_ADDRESS
        --, address.SPATIAL_LOCATION
        , address.MODIFIED_DATE

        from salesterritory
        left join stateprovince
            on salesterritory.pk_territory = stateprovince.fk_territory
        left join address
            on stateprovince.pk_state_province = address.fk_state_province
        left join countryregion
            on stateprovince.fk_country_region_code = countryregion.pk_country_region_code
    )

select * 
from joined    