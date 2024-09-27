-- 6sense Buying Stage Movement
--CREATE OR REPLACE TABLE `jellyvision.db_6sense_buying_stages_movement` AS
TRUNCATE TABLE `jellyvision.db_6sense_buying_stages_movement`;

INSERT INTO `jellyvision.db_6sense_buying_stages_movement` (
    _6sensecountry,	
    _6sensedomain,	
    _6sensecompanyname,	
    _activities_on,	
    _source,	
    _country_account,	
    _current_stage,	
    _prev_stage,	
    _curr_order,	
    _prev_order,	
    _movement
)
-- Set buying stages and their order
WITH stage_order AS (
    SELECT
      'Target' AS _buying_stage,
      1 AS _order
    UNION ALL
    SELECT
      'Awareness' AS _buying_stage,
      2
    UNION ALL
    SELECT
      'Consideration' AS _buying_stage,
      3
    UNION ALL
    SELECT
      'Decision' AS _buying_stage,
      4
    UNION ALL
    SELECT
      'Purchase' AS _buying_stage,
      5
  ),
  -- Get buying stage data
  buying_stage_data AS (
    SELECT DISTINCT
      _6sensecompanyname,
      _6sensecountry,
      _6sensedomain,
      _buyingstageend AS _buying_stage,
      CASE
        WHEN _extractdate LIKE '0%' THEN PARSE_DATE('%m/%d/%y', _extractdate)
        ELSE PARSE_DATE('%m/%d/%Y', _extractdate)
      END AS _activities_on
    FROM `x-marketing.jellyvision_mysql.jellyvision_db_account_initial_buying_stage`
    WHERE LENGTH(_extractdate) > 0
  ),
  buying_stages AS (
    SELECT DISTINCT
      _6sensecountry,
      _6sensecompanyname,
      MIN(_activities_on) AS _activities_on
    FROM buying_stage_data
    GROUP BY
      1,
      2
  ),
  -- Get first ever buying stage for each account
  first_ever_buying_stage AS (
    SELECT DISTINCT
      _activities_on,
      _6sensecountry,
      _6sensedomain,
      _6sensecompanyname,
      _buying_stage,
      'Initial' AS _source,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM buying_stage_data
    JOIN buying_stages
      USING (_6sensecountry, _6sensecompanyname, _activities_on)
  ),
  buying_stage_datas AS (
    SELECT DISTINCT
      _activities_on,
      _6sensecountry,
      _6sensedomain,
      _6sensecompanyname,
      _buying_stage,
      'Non Initial' AS _source,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM buying_stage_data
  ),
  -- Get every other buying stage for each account
  every_other_buying_stage AS (
    SELECT
      *
    FROM buying_stage_datas -- Exclude those that are first ever stages
    WHERE CONCAT(_country_account, _activities_on) NOT IN (
      SELECT DISTINCT
        CONCAT(_country_account, MIN(_activities_on))
      FROM first_ever_buying_stage
      GROUP BY
        _country_account
    )
  ),
  -- Combine both first ever data and every other data
  historical_buying_stage AS (
    SELECT
      *
    FROM first_ever_buying_stage
    UNION DISTINCT
    SELECT
      *
    FROM every_other_buying_stage
  ),
  buying_stage_order AS (
    SELECT DISTINCT
      _6sensecountry,
      _6sensedomain,
      _6sensecompanyname,
      _buying_stage AS _current_stage,
      _activities_on,
      LAG(_buying_stage) OVER (
        PARTITION BY
          _6sensedomain
        ORDER BY
          _activities_on ASC
      ) AS _prev_stage,
      _source,
      _country_account
    FROM historical_buying_stage
  ),
  -- Get the current stage and previous stage for each historical record of an account
  set_buying_stage_order AS (
    SELECT DISTINCT
      buying_stage_order.* EXCEPT (_current_stage, _prev_stage),
      buying_stage_order._current_stage,
      IF(
        _activities_on = (
          MIN(_activities_on) OVER (
            PARTITION BY
              _6sensedomain,
              _6sensecountry
            ORDER BY
              _activities_on
          )
        )
        AND _prev_stage IS NULL,
        _current_stage,
        _prev_stage
      ) AS _prev_stage,
      curr._order AS _curr_order,
      IF(
        _activities_on = (
          MIN(_activities_on) OVER (
            PARTITION BY
              _6sensedomain,
              _6sensecountry
            ORDER BY
              _activities_on
          )
        )
        AND _prev_stage IS NULL,
        curr._order,
        prev._order
      ) AS _prev_order
    FROM buying_stage_order
    LEFT JOIN stage_order AS curr
      ON buying_stage_order._current_stage = curr._buying_stage
    LEFT JOIN stage_order AS prev
      ON buying_stage_order._prev_stage = prev._buying_stage
  ),
  -- Set movement of each historical record an account
  set_movement AS (
    SELECT DISTINCT
      *,
      IF(
        _curr_order > _prev_order,
        "+ve",
        IF(_curr_order < _prev_order, "-ve", "Stagnant")
      ) AS _movement,
    FROM set_buying_stage_order
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        _country_account
      ORDER BY
        _activities_on DESC
    ) = 1
  )
SELECT
  *
FROM set_movement
ORDER BY
  _activities_on DESC,
  _country_account;