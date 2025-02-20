with 
    dim_products as (
        select *
        from {{ ref('int_vendas_info_products') }}
    )

select * 
from dim_products