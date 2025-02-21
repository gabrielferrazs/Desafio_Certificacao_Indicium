with
    dim_sales_reason as (
        select *
        from {{ ref('int_vendas_info_reason_sales') }}
    )

select *
from dim_sales_reason