with
    dim_dates as (
        select *
        from {{ ref('int_vendas_info_dates') }}
    )

select *
from dim_dates