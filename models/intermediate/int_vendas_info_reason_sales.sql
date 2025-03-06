with
   salesreason as (
        select * 
        from {{ ref('stg_erp__salesreason') }}
    )

,   salesorderheadersalesreason as (
        select * 
        from {{ ref('stg_erp__salesorderheadersalesreason') }}
    )

,   join_reason as (
        select
            coalesce(salesorderheadersalesreason.pk_sales_order || ' ', '') as pk_sales_order
            , salesreason.pk_sales_reason
            , salesreason.reason_type
            , case 
                when salesreason.pk_sales_reason in (2) then 'Promotion'
                when salesreason.pk_sales_reason in (3, 4, 7, 8) then 'Marketing'
                when salesreason.pk_sales_reason in (1, 5, 6, 9, 10) then 'Other'
                else 'Sem Categoria'
            end as motivo
            , row_number() over (partition by salesorderheadersalesreason.pk_sales_order order by salesreason.pk_sales_reason) as rn
        from salesreason
        left join salesorderheadersalesreason
            on salesreason.pk_sales_reason = salesorderheadersalesreason.fk_sales_reason
        group by salesorderheadersalesreason.pk_sales_order
                , motivo
                , salesreason.pk_sales_reason
                , salesreason.reason_type
    )

,   final as (
        select 
            pk_sales_order
            --, reason_type
            , motivo as reason
        from join_reason
        where rn = 1
)

select *
from final