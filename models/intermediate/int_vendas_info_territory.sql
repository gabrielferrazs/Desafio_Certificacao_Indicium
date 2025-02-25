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
        address.pk_address as pk_address
        , salesterritory.pk_territory
        , address.addressline1		
        , address.addressline2
        , address.name_city
        , address.postal_code
        , stateprovince.name_state
        , stateprovince.state_province_code
        , countryregion.name_country
        , salesterritory.country_code
        , salesterritory.region
        from address
        left join stateprovince
            on address.fk_state_province = stateprovince.pk_state_province
        left join countryregion
            on stateprovince.fk_country_region_code = countryregion.pk_country_region_code
        left join salesterritory
            on stateprovince.fk_territory = salesterritory.pk_territory

    )

select * 
from joined    