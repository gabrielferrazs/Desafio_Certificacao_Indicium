with source as (
        select * from {{ source('erp', 'personcreditcard') }}
  ),
  renamed as (
      select
        cast(businessentityid as int) as pk_business_entity
        , cast(creditcardid as int) as fk_credit_card
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    