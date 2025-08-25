-- Before segment: only create if there's actual time before updateStart
SELECT
    {{.ColumnsString }},
    DATETIME (valid_open)               valid_open,
    DATETIME (@valid_open)              valid_close,
    DATETIME (current_timestamp)        txn_open,
    DATETIME ('9999-12-31 23:59:59')    txn_close
FROM {{.Table }}
WHERE {{ .FiltersString }}
  AND DATETIME(valid_close) > DATETIME(@valid_open)
  AND DATETIME(valid_open) < DATETIME(@valid_open)  -- Ensure non-zero duration
  AND DATETIME(txn_open) <= current_timestamp
  AND DATETIME(txn_close) >= current_timestamp
UNION ALL
-- Update segment: the new values for the update window
SELECT
    {{.ColumnParamsString }},
    DATETIME (CASE WHEN DATETIME(valid_open)    <   DATETIME(@valid_open)   THEN DATETIME(@valid_open)  ELSE DATETIME(valid_open)   END) valid_open,
    DATETIME (CASE WHEN DATETIME(valid_close)      >=  DATETIME(@valid_close)     THEN DATETIME(@valid_close)    ELSE DATETIME(valid_close)     END) valid_close,
    DATETIME (current_timestamp)        txn_open,
    DATETIME ('9999-12-31 23:59:59')    txn_close
    FROM {{.Table }}
    WHERE {{ .FiltersString }}
    AND DATETIME(valid_open) < @valid_close
    AND DATETIME(valid_close) > @valid_open
    AND DATETIME(txn_open) <= current_timestamp
    AND DATETIME(txn_close) >= current_timestamp
-- Ensure the calculated period has positive duration
    AND   DATETIME(CASE WHEN DATETIME(valid_open)   <   DATETIME(@valid_open)   THEN @valid_open    ELSE valid_open END)
        < DATETIME(CASE WHEN DATETIME(valid_close)     >=  DATETIME(@valid_close)     THEN @valid_close      ELSE valid_close   END)
UNION ALL
-- After segment: preserve portions of records that extend beyond updateEnd
-- Only for records that START BEFORE updateEnd but extend beyond it
SELECT
    {{.ColumnsString }},
    DATETIME (@valid_close)                valid_open,
    DATETIME (valid_close)                 valid_close,
    DATETIME (current_timestamp)        txn_open,
    DATETIME ('9999-12-31 23:59:59')    txn_close
FROM {{.Table }}
WHERE {{ .FiltersString }}
AND DATETIME(valid_open) < DATETIME(@valid_close)          -- Must start BEFORE updateEnd
AND DATETIME(valid_close) > DATETIME(@valid_close)            -- Must end AFTER updateEnd
AND DATETIME(@valid_close) < DATETIME(valid_close) -- Ensure positive duration
-- Explicit exclusion: do not include records that start exactly at updateEnd
AND DATETIME(valid_open) != DATETIME(@valid_close)
AND DATETIME(txn_open) <= current_timestamp
AND DATETIME(txn_close) >= current_timestamp
UNION ALL
-- New period segment: create new record when update window has no overlap with existing data
-- This handles cases where the update is entirely before or after existing data
SELECT
    {{.ColumnParamsString }},
    DATETIME (@valid_open)              valid_open,
    DATETIME (@valid_close)                valid_close,
    DATETIME (current_timestamp)        txn_open,
    DATETIME ('9999-12-31 23:59:59')    txn_close
WHERE NOT EXISTS (
SELECT 1 FROM {{.Table }}
WHERE {{ .FiltersString }}
    AND DATETIME(valid_open) < DATETIME(@valid_close)
    AND DATETIME(valid_close) > DATETIME(@valid_open)
    AND DATETIME(txn_open) <= current_timestamp
    AND DATETIME(txn_close) >= current_timestamp
)
UNION ALL
-- Extension segment: create record for portion of update window before the earliest existing data
SELECT
    {{.ColumnParamsString }},
    DATETIME (@valid_open) valid_open,
    (
        SELECT DATETIME(MIN(valid_open)) valid_open
        FROM {{.Table }}
        WHERE {{ .FiltersString }} AND DATETIME(txn_open) <= current_timestamp AND DATETIME(txn_close) >= current_timestamp
    ) as valid_close,
    DATETIME (current_timestamp)        txn_open,
    DATETIME ('9999-12-31 23:59:59')    txn_close
WHERE DATETIME(@valid_open) < (
    SELECT MIN(valid_open) FROM {{.Table }}
    WHERE {{ .FiltersString }}
    AND DATETIME(txn_open) <= current_timestamp
    AND DATETIME(txn_close) >= current_timestamp
) AND EXISTS (
    SELECT 1 FROM {{.Table }}
    WHERE {{ .FiltersString }}
        AND DATETIME(valid_open) < DATETIME(@valid_close)
        AND DATETIME(valid_close) > DATETIME(@valid_open)
        AND DATETIME(txn_open) <= DATETIME(current_timestamp)
        AND DATETIME(txn_close) >= DATETIME(current_timestamp)
)
ORDER BY valid_open