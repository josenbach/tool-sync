SELECT gm.TOOL_NUM                                                                   AS serial_number
    , CASE 
        WHEN gm.PART_NUMBER IS NULL OR LTRIM(RTRIM(gm.PART_NUMBER)) = '' OR LTRIM(RTRIM(gm.PART_NUMBER)) = ' ' 
        THEN CASE 
            WHEN gm.MODEL_NUM IS NULL OR LTRIM(RTRIM(gm.MODEL_NUM)) = '' OR LTRIM(RTRIM(gm.MODEL_NUM)) = ' '
            THEN NULL
            ELSE LTRIM(RTRIM(gm.MODEL_NUM))
        END
        ELSE LTRIM(RTRIM(gm.PART_NUMBER))
      END                                                                            AS part_number                      
    , gm.TOOL_NUM_DESC                                                               AS description
    , IIF(gm.PART_REVISION IS NULL OR gm.PART_REVISION = ' ', 'A', gm.PART_REVISION) AS revision
    , CAST(
        IIF(gm.FREQUENCY_TYPE = 'Months',
            gm.FREQUENCY * 30.4375 * 24 * 60 * 60,
            IIF(gm.FREQUENCY_TYPE = 'Weeks',
                gm.FREQUENCY * 7 * 24 * 60 * 60,
                IIF(gm.FREQUENCY_TYPE = 'Days',
                    gm.FREQUENCY * 24 * 60 * 60,
                    NULL
                    )
                )
            ) AS INT
        )                                                                            AS service_interval_seconds
    , gm.GTYPE                                                                       AS asset_type
    , gm.LOCATION_CODE                                                               AS location
    , FORMAT(gm.LAST_CAL_DATE, 'yyyy-MM-dd HH:mm:ss')                                AS last_maintenance_date
    , gm.MANUFACTURER_SN                                                             AS asset_serial_number
    , gm.MANUFACTURER                                                                AS manufacturer
    , gm.GT_STATUS_CODE                                                              AS maintenance_status
    , gm.R_STATUS                                                                    AS revision_status
FROM GT_MASTER gm WITH (NOLOCK)
WHERE gm.BUSINESS_UNIT = 'JAI'