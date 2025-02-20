with 
    customer as (
        select *
        from {{ ref('stg_erp__customer') }}
    )

,   person as (
    select * 
    from {{ ref('stg_erp__person') }}
    )

select 
person.PK_BUSINESS_ENTITY
, person.PK_PERSON
--, customer.pk_customer
--, customer.fk_person
--, customer.fk_store
--, customer.fk_territory
--, person.PERSON_TYPE
--, person.NAME_STYLE
, person.TITLE
--, person.FIRST_NAME
--, person.MIDDLE_NAME
--, person.LAST_NAME
, person.FIRST_NAME || ' ' || coalesce(person.middle_name || ' ', '') || person.LAST_NAME as full_name
--, person.SUFFIX
--, person.EMAIL_PROMOTION
--, person.ADDITIONAL_CONTACT_INFO
--, person.DEMOGRAPHICS
--, person.ROWGUID	
, person.MODIFIED_DATE

--, customer.rowguid
--, customer.modified_date
from person
inner join customer
    on person.pk_person = customer.fk_person
