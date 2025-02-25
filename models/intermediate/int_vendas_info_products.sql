with 
    product as (
        select *
        from {{ ref('stg_erp__product') }}
    )

,   productsubcategory as (
        select * 
        from {{ ref('stg_erp__productsubcategory') }}
 
    )

,   productcategory as (
        select *
        from {{ ref('stg_erp__productcategory') }}
    )
       
,   joined as (
        select
        product.pk_product	
        , product.name_product
        , product.product_number
        , productcategory.name_category
        , productsubcategory.name_sub_category
        
        
        from product
        left join productsubcategory 
            on product.fk_product_subcategory = productsubcategory.pk_product_subcategory 
        left join productcategory
            on productsubcategory.fk_product_category = productcategory.pk_product_category
    )

select * from joined