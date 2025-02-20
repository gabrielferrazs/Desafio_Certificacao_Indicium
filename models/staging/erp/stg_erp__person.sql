with source as (
        select * from {{ source('erp', 'person') }}
  ),
  renamed as (
      select
        cast(businessentityid as int) as pk_business_entity
        , cast(businessentityid as int) as pk_person
        , cast(persontype as varchar) as person_type
        , cast(namestyle as boolean) as name_style
        , cast(title as varchar) as title
        , cast(firstname as varchar) as first_name
        , cast(middlename as varchar) as middle_name
        , cast(lastname as varchar) as last_name
        , cast(suffix as varchar) as suffix
        , cast(emailpromotion as numeric) as email_promotion
        , cast(additionalcontactinfo as varchar) as additional_contact_info
        , cast(demographics as varchar) as demographics
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    