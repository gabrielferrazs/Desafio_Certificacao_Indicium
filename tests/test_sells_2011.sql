with
    sells_2011 as (
        select
        sum(valor_total_negociado) as soma_total_bruto
        from {{ ref('int_vendas_info_sales') }}
        where order_date between '2011-01-01' and '2011-12-31'
    )
select soma_total_bruto
from sells_2011
where soma_total_bruto not between 12646112.00 and 12646113.00
