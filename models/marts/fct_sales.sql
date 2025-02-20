with
    fct_sales as (
        select *
        from {{ ref('int_vendas_info_sales') }}
    )

select *
from fct_sales     