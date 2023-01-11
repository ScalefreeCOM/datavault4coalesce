INSERT INTO
  COALESCE_WORKSHOP.CORE.SAT_ORDERS1 WITH latest_entries_in_sat AS (
    /* get current rows from satellite */
    SELECT
      hk_order_h,
      hd_order_s
    FROM
      COALESCE_WORKSHOP.CORE.SAT_ORDERS1 QUALIFY ROW_NUMBER() OVER (
        PARTITION BY hk_order_h
        ORDER BY
          ldts DESC
      ) = 1
  ),
  deduplicated_numbered_source AS (
    SELECT
      STG_ORDERS1.hk_order_h AS hk_order_h,
      STG_ORDERS1.hd_order_s AS hd_order_s,
      STG_ORDERS1.O_ORDERSTATUS AS O_ORDERSTATUS,
      STG_ORDERS1.O_TOTALPRICE AS O_TOTALPRICE,
      STG_ORDERS1.O_ORDERDATE AS O_ORDERDATE,
      STG_ORDERS1.O_ORDERPRIORITY AS O_ORDERPRIORITY,
      STG_ORDERS1.O_CLERK AS O_CLERK,
      STG_ORDERS1.O_SHIPPRIORITY AS O_SHIPPRIORITY,
      STG_ORDERS1.O_COMMENT AS O_COMMENT,
      STG_ORDERS1.rsrc AS rsrc,
      STG_ORDERS1.ldts AS ldts,
      ROW_NUMBER() OVER(
        PARTITION BY hk_order_h
        ORDER BY
          ldts
      ) as rn
    FROM
      COALESCE_WORKSHOP.CORE.STG_ORDERS1 STG_ORDERS1 QUALIFY CASE
        WHEN hd_order_s = LAG(hd_order_s) OVER(
          PARTITION BY hk_order_h
          ORDER BY
            ldts
        ) THEN FALSE
        ELSE TRUE
      END
  ),
SELECT
  DISTINCT STG_ORDERS1.hk_order_h AS hk_order_h,
  STG_ORDERS1.hd_order_s AS hd_order_s,
  STG_ORDERS1.O_ORDERSTATUS AS O_ORDERSTATUS,
  STG_ORDERS1.O_TOTALPRICE AS O_TOTALPRICE,
  STG_ORDERS1.O_ORDERDATE AS O_ORDERDATE,
  STG_ORDERS1.O_ORDERPRIORITY AS O_ORDERPRIORITY,
  STG_ORDERS1.O_CLERK AS O_CLERK,
  STG_ORDERS1.O_SHIPPRIORITY AS O_SHIPPRIORITY,
  STG_ORDERS1.O_COMMENT AS O_COMMENT,
  STG_ORDERS1.rsrc AS rsrc,
  STG_ORDERS1.ldts AS ldts
FROM
  deduplicated_numbered_source
WHERE
  NOT EXISTS (
    SELECT
      1
    FROM
      latest_entries_in_sat
    WHERE
      STG_ORDERS1.hk_order_h = latest_entries_in_sat.hk_order_h
      AND STG_ORDERS1.hd_order_s = latest_entries_in_sat.hd_order_s
      AND deduplicated_numbered_source.rn = 1
  )