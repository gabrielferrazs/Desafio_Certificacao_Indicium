with 
    creditcard as (
        select *
        from {{ ref('stg_erp__creditcard') }}
    )

,       personcreditcard as (
        select *
        from {{ ref('stg_erp__personcreditcard') }}
    )

,   joined as (
        select 
            creditcard.pk_credit_card
            --, personcreditcard.pk_business_entity
            , creditcard.card_type
        from creditcard
        left join personcreditcard 
            on creditcard.pk_credit_card = personcreditcard.fk_credit_card
    )

select * 
from joined    