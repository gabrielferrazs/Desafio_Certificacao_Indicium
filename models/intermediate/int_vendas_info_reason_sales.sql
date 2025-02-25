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
            , CASE 
                WHEN salesreason.pk_sales_reason IN (2) THEN 'Promotion'
                WHEN salesreason.pk_sales_reason IN (3, 4, 7, 8) THEN 'Marketing'
                WHEN salesreason.pk_sales_reason IN (1, 5, 6, 9, 10) THEN 'Other'
                ELSE 'Teste'
            END AS motivo
            , ROW_NUMBER() OVER (PARTITION BY salesorderheadersalesreason.pk_sales_order ORDER BY salesreason.pk_sales_reason) as rn
        FROM salesreason
        LEFT JOIN salesorderheadersalesreason
            ON salesreason.pk_sales_reason = salesorderheadersalesreason.fk_sales_reason
        GROUP BY salesorderheadersalesreason.pk_sales_order
                , motivo
                , salesreason.pk_sales_reason
                , salesreason.reason_type
                
    )

select 
pk_sales_order
--, reason_type
, motivo as reason
from join_reason
where rn = 1
