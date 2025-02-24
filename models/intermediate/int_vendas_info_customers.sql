with 
    customer as (
        select *
        from {{ ref('stg_erp__customer') }}
    )

,   person as (
        select * 
        from {{ ref('stg_erp__person') }}
    )

,   join_customers as (
        select 
        --person.pk_business_entity
        --, person.pk_person
        customer.pk_customer
        , person.title
        , person.first_name || ' ' || coalesce(person.middle_name || ' ', '') || person.last_name as full_name

        from person
        left join customer
            on person.pk_business_entity = customer.fk_person
    )

select * 
from join_customers