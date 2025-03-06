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

        from customer
        left join person
            on customer.pk_customer = person.pk_person
    )

,   criando_sk as (
        select 
            md5(coalesce(pk_customer || ' ', '')  || full_name) as sk_customer 
            , pk_customer
            , title
            , full_name
        from join_customers
    )

select * 
from join_customers