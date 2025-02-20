with
    dim_customer as (
        select *
        from {{ ref('int_vendas_info_customers') }}
    )

select *
from dim_customer     