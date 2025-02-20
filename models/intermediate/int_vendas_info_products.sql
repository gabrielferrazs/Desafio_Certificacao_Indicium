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
--, product.FK_PRODUCT_SUBCATEGORY
--, product.FK_PRODUCT_MODEL
, product.NAME_PRODUCT
, product.PRODUCT_NUMBER
--, product.MAKE_FLAG
--, product.FINISHED_GOODS_FLAG
, productcategory.NAME_CATEGORY
, productsubcategory.NAME_SUB_CATEGORY
, product.COLOR
, product.SAFETY_STOCK_LEVEL
, product.REORDER_POINT
, product.STANDARD_COST
, product.LIST_PRICE
, product.SIZE
, product.SIZE_UNIT_MEASURE_CODE
, product.WEIGHT_UNIT_MEASURE_CODE
, product.WEIGHT
, product.DAYS_TO_MANUFACTURE	
, product.PRODUCT_LINE
, product.CLASS
, product.STYLE
--, product.SELL_START_DATE
--, product.SELL_END_DATE	
, product.DISCONTINUED_DATE
--, product.ROWGUID
, product.MODIFIED_DATE
--, productcategory.PK_PRODUCT_CATEGORY
--, productcategory.ROWGUID as ROWGUID_C
--, productcategory.MODIFIED_DATE as MODIFIED_DATE_C
--, productsubcategory.PK_PRODUCT_SUBCATEGORY
--, productsubcategory.FK_PRODUCT_CATEGORY
--, productsubcategory.ROWGUID as ROWGUID_SUBC
--, productsubcategory.MODIFIED_DATE as MODIFIED_DATE_SUBC

from product
left join productsubcategory 
    on product.fk_product_subcategory = productsubcategory.pk_product_subcategory 
left join productcategory
    on productsubcategory.fk_product_category = productcategory.pk_product_category
    )

select * from joined