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
       
, joined as (
        select
        product.PK_PRODUCT	
        , product.NAME_PRODUCT
        , product.PRODUCT_NUMBER
        , productcategory.NAME_CATEGORY
        , productsubcategory.NAME_SUB_CATEGORY
        
        
        from product
        left join productsubcategory 
            on product.fk_product_subcategory = productsubcategory.pk_product_subcategory 
        left join productcategory
            on productsubcategory.fk_product_category = productcategory.pk_product_category
    )

select * from joined