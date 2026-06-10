
----- crear tabla bronze clientes

CREATE OR REFRESH STREAMING TABLE IDENTIFIER('${CATALOG}.ly_bronze.CLIENTS_RW') (
    CLIENT_ID STRING,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    PHONE STRING,
    CITY STRING,
    STATE STRING,
    ZIP_CODE INTEGER,
    SOURCE_FILE STRING,  -- METADATA
    FILE_MOD_TIME TIMESTAMP  --- METADAT
)
COMMENT 'Tabla Bronze que almacena el contenido crudo de clientes en CSV'
AS
SELECT 
    CLIENT_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    CITY,
    STATE,
    ZIP_CODE,
    _metadata.file_name AS SOURCE_FILE,
    _metadata.file_modification_time AS FILE_MOD_TIME
FROM STREAM read_files(
    "/Volumes/${CATALOG}/ly_bronze/raw/CLIENTS",
    format => 'csv',
    header => 'true',
    delimiter => ',',
    multiLine => 'true'
);


--------- CREAR TABLA ORDERS

CREATE OR REFRESH STREAMING TABLE IDENTIFIER('${CATALOG}.ly_bronze.ORDERS_RW') (
    ORDER_ID STRING,
    ORDER_TIMESTAMP TIMESTAMP,
    ORDER_DATE DATE,
    CLIENT_ID STRING,
    CATEGORY STRING,
    TOTAL_QTY INTEGER,
    TOTAL_PRICE STRING,
    SOURCE_FILE STRING,
    FILE_MOD_TIME TIMESTAMP
)
COMMENT "Tabla que almacena los datos de pedidos de diferentes fuentes";

---- INSERT INTO ORDERS BR 
---- CSV 
CREATE FLOW LOAD_CSV_INTO_ORDERS 
AS INSERT INTO IDENTIFIER('${CATALOG}.ly_bronze.ORDERS_RW') BY NAME 
SELECT 
    ORDER_ID,
    TO_TIMESTAMP( ORDER_TIMESTAMP, "MM-dd-yyyy HH:mm:ss") as order_timestamp,
    TO_DATE( ORDER_DATE, "MM-dd-yyyy") AS ORDER_DATE,
    CLIENT_ID,
    CATEGORY,
    TOTAL_QTY,
    NULLIF(TRIM(TOTAL_PRICE), '') AS TOTAL_PRICE,
    _metadata.file_name AS SOURCE_FILE,
    _metadata.file_modification_time AS FILE_MOD_TIME
FROM STREAM read_files(
    "/Volumes/${CATALOG}/ly_bronze/raw/Orders/",
    format => "csv",
    header => "true",
    delimiter => ",",
    multiLine => "true",
    pathGlobFilter => "*.csv"
);

---- JSON INPUT
CREATE FLOW LOAD_JSON_INTO_ORDERS 
AS INSERT INTO IDENTIFIER('${CATALOG}.ly_bronze.ORDERS_RW') BY NAME 
WITH raw_json AS (
    SELECT 
        value AS raw_content,
        _metadata.file_name AS source_file,
        _metadata.file_modification_time AS file_mod_time
    FROM STREAM read_files(
        "/Volumes/${CATALOG}/ly_bronze/raw/Orders/",
        format => "text",
        wholeText => "true",
        pathGlobFilter => "*.json"
    )
),
parsed_json AS (
    SELECT
        explode(
            from_json(
                raw_content,
                'ARRAY<STRUCT<ORDER_ID:STRING,ORDER_TIMESTAMP:STRING,ORDER_DATE:STRING,CLIENT_ID:STRING,CATEGORY:STRING,TOTAL_QTY:INT,TOTAL_PRICE:DOUBLE>>'
            )
        ) AS order_record,
        source_file,
        file_mod_time
    FROM raw_json
)
SELECT
    order_record.ORDER_ID AS ORDER_ID,
    TO_TIMESTAMP(order_record.ORDER_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS ORDER_TIMESTAMP,
    TO_DATE(order_record.ORDER_DATE, 'yyyy-MM-dd') AS ORDER_DATE,
    order_record.CLIENT_ID AS CLIENT_ID,
    order_record.CATEGORY AS CATEGORY,
    order_record.TOTAL_QTY AS TOTAL_QTY,
    CAST(order_record.TOTAL_PRICE AS STRING) AS TOTAL_PRICE,
    source_file AS SOURCE_FILE,
    file_mod_time AS FILE_MOD_TIME
FROM parsed_json;

