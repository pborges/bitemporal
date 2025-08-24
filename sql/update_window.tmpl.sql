-- Before segment: only create if there's actual time before updateStart
SELECT
    {{.ColumnsString }},
    DATETIME (valid_from)               valid_from,
    DATETIME (@valid_from)              valid_to,
    DATETIME (current_timestamp)        transaction_from,
    DATETIME ('9999-12-31 23:59:59')    transaction_to
FROM {{.Table }}
WHERE {{ .FiltersString }}
  AND DATETIME(valid_to) > DATETIME(@valid_from)
  AND DATETIME(valid_from) < DATETIME(@valid_from)  -- Ensure non-zero duration
  AND DATETIME(transaction_from) <= current_timestamp
  AND DATETIME(transaction_to) >= current_timestamp
UNION ALL
-- Update segment: the new values for the update window
SELECT
    {{.ColumnParamsString }},
    DATETIME (CASE WHEN DATETIME(valid_from)    <   DATETIME(@valid_from)   THEN DATETIME(@valid_from)  ELSE DATETIME(valid_from)   END) valid_from,
    DATETIME (CASE WHEN DATETIME(valid_to)      >=  DATETIME(@valid_to)     THEN DATETIME(@valid_to)    ELSE DATETIME(valid_to)     END) valid_to,
    DATETIME (current_timestamp)        transaction_from,
    DATETIME ('9999-12-31 23:59:59')    transaction_to
    FROM {{.Table }}
    WHERE {{ .FiltersString }}
    AND DATETIME(valid_from) < @valid_to
    AND DATETIME(valid_to) > @valid_from
    AND DATETIME(transaction_from) <= current_timestamp
    AND DATETIME(transaction_to) >= current_timestamp
-- Ensure the calculated period has positive duration
    AND   DATETIME(CASE WHEN DATETIME(valid_from)   <   DATETIME(@valid_from)   THEN @valid_from    ELSE valid_from END)
        < DATETIME(CASE WHEN DATETIME(valid_to)     >=  DATETIME(@valid_to)     THEN @valid_to      ELSE valid_to   END)
UNION ALL
-- After segment: preserve portions of records that extend beyond updateEnd
-- Only for records that START BEFORE updateEnd but extend beyond it
SELECT
    {{.ColumnsString }},
    DATETIME (@valid_to)                valid_from,
    DATETIME (valid_to)                 valid_to,
    DATETIME (current_timestamp)        transaction_from,
    DATETIME ('9999-12-31 23:59:59')    transaction_to
FROM {{.Table }}
WHERE {{ .FiltersString }}
AND DATETIME(valid_from) < DATETIME(@valid_to)          -- Must start BEFORE updateEnd
AND DATETIME(valid_to) > DATETIME(@valid_to)            -- Must end AFTER updateEnd
AND DATETIME(@valid_to) < DATETIME(valid_to) -- Ensure positive duration
-- Explicit exclusion: do not include records that start exactly at updateEnd
AND DATETIME(valid_from) != DATETIME(@valid_to)
AND DATETIME(transaction_from) <= current_timestamp
AND DATETIME(transaction_to) >= current_timestamp
UNION ALL
-- New period segment: create new record when update window has no overlap with existing data
-- This handles cases where the update is entirely before or after existing data
SELECT
    {{.ColumnParamsString }},
    DATETIME (@valid_from)              valid_from,
    DATETIME (@valid_to)                valid_to,
    DATETIME (current_timestamp)        transaction_from,
    DATETIME ('9999-12-31 23:59:59')    transaction_to
WHERE NOT EXISTS (
SELECT 1 FROM {{.Table }}
WHERE {{ .FiltersString }}
    AND DATETIME(valid_from) < DATETIME(@valid_to)
    AND DATETIME(valid_to) > DATETIME(@valid_from)
    AND DATETIME(transaction_from) <= current_timestamp
    AND DATETIME(transaction_to) >= current_timestamp
)
UNION ALL
-- Extension segment: create record for portion of update window before the earliest existing data
SELECT
    {{.ColumnParamsString }},
    DATETIME (@valid_from) valid_from,
    (
        SELECT DATETIME(MIN(valid_from)) valid_from
        FROM {{.Table }}
        WHERE {{ .FiltersString }} AND DATETIME(transaction_from) <= current_timestamp AND DATETIME(transaction_to) >= current_timestamp
    ) as valid_to,
    DATETIME (current_timestamp)        transaction_from,
    DATETIME ('9999-12-31 23:59:59')    transaction_to
WHERE DATETIME(@valid_from) < (
    SELECT MIN(valid_from) FROM {{.Table }}
    WHERE {{ .FiltersString }}
    AND DATETIME(transaction_from) <= current_timestamp
    AND DATETIME(transaction_to) >= current_timestamp
) AND EXISTS (
    SELECT 1 FROM {{.Table }}
    WHERE {{ .FiltersString }}
        AND DATETIME(valid_from) < DATETIME(@valid_to)
        AND DATETIME(valid_to) > DATETIME(@valid_from)
        AND DATETIME(transaction_from) <= DATETIME(current_timestamp)
        AND DATETIME(transaction_to) >= DATETIME(current_timestamp)
)
ORDER BY valid_from