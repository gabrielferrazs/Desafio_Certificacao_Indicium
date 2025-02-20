with 
    dim_payments as (
        select *
        from {{ ref('int_vendas_info_payments') }}
    )

select * 
from dim_payments