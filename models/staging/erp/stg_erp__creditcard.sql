with source as (
        select * from {{ source('erp', 'creditcard') }}
  ),
  renamed as (
      select
        cast(creditcardid as int) as pk_credit_card
        , cast(cardtype as varchar) as card_type
        , cast(cardnumber as varchar) as card_number
        , cast(expmonth as varchar) as exp_month
        , cast(expyear as varchar) as exp_year
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    