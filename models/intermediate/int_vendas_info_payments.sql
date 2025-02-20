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
        creditcard.PK_CREDIT_CARD
        , personcreditcard.PK_BUSINESS_ENTITY
        , creditcard.CARD_TYPE
        --, creditcard.CARD_NUMBER
        --, creditcard.EXP_MONTH
        --, creditcard.EXP_YEAR
        , creditcard.MODIFIED_DATE
        --, personcreditcard.MODIFIED_DATE
        from creditcard
        left join personcreditcard
            on creditcard.pk_credit_card = personcreditcard.fk_credit_card
    )

select * 
from joined    