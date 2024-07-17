with 

source as (

    select
        date,
        PARTNER,
        PRODUCT,
        REPORTER,
        VALUE_IN_EUROS,
        QUANTITY_IN_100KG

    from {{ source('sh_stg', 'ds_045409') }}


    where TRY_TO_NUMBER(QUANTITY_IN_100KG) is not NULL
),

renaming as (

    select
        TO_DATE(CONCAT(date, '-01'), 'YYYY-MM-DD') as FECHA,
        PARTNER as PAIS_SOCIO,
        PRODUCT as PRODUCTO,
        REPORTER as PAIS_IMPORTADOR,
        VALUE_IN_EUROS as VALOR_EUR,
        (QUANTITY_IN_100KG::NUMERIC) / 10 AS CANTIDAD,
        'TN' as MEDIDA_CANTIDAD

    from source
),


final as (

    select 
        FECHA,
        PAIS_SOCIO,
        PRODUCTO,
        PAIS_IMPORTADOR,
        VALOR_EUR,
        CANTIDAD,
        MEDIDA_CANTIDAD,
        CURRENT_TIMESTAMP(2) AS ETL_TIMESTAMP,
        {{dbt_utils.generate_surrogate_key(['FECHA','PAIS_SOCIO','PAIS_IMPORTADOR','PRODUCTO'])}} AS PK_ODS_FACT_DS_045409

    from renaming
)


select * from final