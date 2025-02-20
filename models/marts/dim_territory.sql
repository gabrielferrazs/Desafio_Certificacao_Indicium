with
    dim_territory as (
        select *
        from {{ ref('int_vendas_info_territory') }}
    )

select *
from dim_territory     