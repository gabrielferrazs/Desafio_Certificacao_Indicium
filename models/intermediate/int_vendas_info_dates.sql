with
    dates as (
        {{ dbt_date.get_date_dimension("2011-01-01", "2014-12-31") }}
    )

-- Selecionando as colunas para o modelo.
,   colunas as (
        select
            date_day as pk_date
            , day_of_week_name
            , day_of_week_name_short
            , month_of_year
            , month_name
            , month_name_short
            , year_number
        from dates 
)

select *
from colunas