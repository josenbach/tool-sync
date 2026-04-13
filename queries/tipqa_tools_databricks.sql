SELECT gm.TOOL_NUM                                                                    AS serial_number
    , CASE
        WHEN gm.PART_NUMBER IS NULL OR TRIM(gm.PART_NUMBER) = '' OR TRIM(gm.PART_NUMBER) = ' '
        THEN CASE
            WHEN gm.MODEL_NUM IS NULL OR TRIM(gm.MODEL_NUM) = '' OR TRIM(gm.MODEL_NUM) = ' '
            THEN NULL
            ELSE TRIM(gm.MODEL_NUM)
        END
        ELSE TRIM(gm.PART_NUMBER)
      END                                                                              AS part_number
    , gm.TOOL_NUM_DESC                                                                 AS description
    , CASE WHEN gm.PART_REVISION IS NULL OR TRIM(gm.PART_REVISION) = '' THEN 'A'
           ELSE gm.PART_REVISION END                                                   AS revision
    , CAST(
        CASE WHEN gm.FREQUENCY_TYPE = 'Months'
                  THEN gm.FREQUENCY * 30.4375 * 24 * 60 * 60
             WHEN gm.FREQUENCY_TYPE = 'Weeks'
                  THEN gm.FREQUENCY * 7 * 24 * 60 * 60
             WHEN gm.FREQUENCY_TYPE = 'Days'
                  THEN gm.FREQUENCY * 24 * 60 * 60
             ELSE NULL
        END AS INT
      )                                                                                AS service_interval_seconds
    , gm.GTYPE                                                                         AS asset_type
    , gm.LOCATION_CODE                                                                 AS location
    , DATE_FORMAT(gm.LAST_CAL_DATE, 'yyyy-MM-dd HH:mm:ss')                             AS last_maintenance_date
    , gm.MANUFACTURER_SN                                                               AS asset_serial_number
    , gm.MANUFACTURER                                                                  AS manufacturer
    , gm.GT_STATUS_CODE                                                                AS maintenance_status
    , gm.R_STATUS                                                                      AS revision_status
FROM {catalog}.{schema}.{table} gm
WHERE gm.BUSINESS_UNIT = 'JAI'
