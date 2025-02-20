with source as (
        select * from {{ source('erp', 'product') }}
  ),
  renamed as (
      select
        cast(productid as int) as pk_product
        , cast(productsubcategoryid as int) as fk_product_subcategory
        , cast(productmodelid as int) as fk_product_model
        , cast(name as varchar) as name_product
        , cast(productnumber as varchar) as product_number
        , cast(makeflag as boolean) as make_flag
        , cast(finishedgoodsflag as boolean) as finished_goods_flag
        , cast(color as varchar) as color
        , cast(safetystocklevel as int) as safety_stock_level
        , cast(reorderpoint as int) as reorder_point
        , cast(standardcost as number(38,2)) as standard_cost
        , cast(listprice as number(38,2)) as list_price
        , cast(size as varchar) as size
        , cast(sizeunitmeasurecode as varchar) as size_unit_measure_code
        , cast(weightunitmeasurecode as varchar) as weight_unit_measure_code
        , cast(weight as number(38,2)) as weight
        , cast(daystomanufacture as int) as days_to_manufacture
        , cast(productline as varchar) as product_line
        , cast(class as varchar) as class
        , cast(style as varchar) as style
        , cast(sellstartdate as date) as sell_start_date
        , cast(sellenddate as date) as sell_end_date
        , cast(discontinueddate as number) as discontinued_date
        , cast(rowguid as varchar) as rowguid
        , cast(modifieddate as date) as modified_date

      from source
  )
  select * from renamed
    