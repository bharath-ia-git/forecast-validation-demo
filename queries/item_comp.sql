with dbv as (  select  distinct hierarchy_code, fiscal_year_week, scenario1 as percent_off from(
    with prod_table as(
  with base as (
  select
  distinct
  sub_channel as channel,hierarchy_code, current_week as fiscal_year_week, 
  scenario as scenario1,
  written_sales_units as IAF_forecast,
  written_aur as aur_IAF
   --609602 
  -- * --  611403
  from
  balsambrands-20022025.balsam_ingestion_prod.iaf_lvl_1_itemsmart_validated_table
  where sub_channel = 'Hybris'
  and current_week between 202552 and 202653
  ),

  wp_table as (
  select
  distinct
  sub_channel as channel,hierarchy_code, current_week as fiscal_year_week, 
  scenario as scenario1,
  written_sales_units as wp
   --609602 
  -- * --  611403
  from
  balsambrands-20022025.balsam_ingestion_prod.wp_lvl_1_itemsmart_validated_table
  where sub_channel = 'Hybris'
  and current_week between 202552 and 202653
  ),

  ly_table as (
    select
    distinct
    sub_channel as channel,hierarchy_code, current_week as fiscal_year_week, 
    scenario as scenario_25,
    written_sales_units as qty_2025,
    written_aur as aur_2025
    from
      balsambrands-20022025.balsam_ingestion_prod.ly_lvl_1_itemsmart_validated_table
    where sub_channel = 'Hybris'
    and current_week between 202552 and 202653
  ),

  lly_table as(
    select
    distinct
    sub_channel as channel,hierarchy_code, current_week as fiscal_year_week, 
    scenario as scenario_24,
    written_sales_units as qty_2024,
    written_aur as aur_2024
    from
      balsambrands-20022025.balsam_ingestion_prod.lly_lvl_1_itemsmart_validated_table
    where sub_channel = 'Hybris'
    and current_week between 202552 and 202653
  ),

  prod_master as 
  (
    SELECT DISTINCT 
    pm.l0_name, pm.l1_name, pm.l2_name, pm.l3_name, pm.l4_name, pm.l5_name,
    pm.sku  as product_code, pm.product_id, ph.hierarchy_code
    FROM balsambrands-20022025.balsam_ingestion_prod.product_master pm
    JOIN balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened ph
    ON pm.l0_name = ph.l0_name
    AND pm.sku = ph.sku
    WHERE ph.level = 7
    AND ph.active = TRUE and pm.active = true)


    select
    concat(a.channel,'-',l0_name) as l0_name,
    concat(a.channel,'-',l0_name,'-',l1_name) as l1_name,
    concat(a.channel,'-',l0_name,'-',l1_name,'-',l2_name) as l2_name,
    concat(a.channel,'-',l0_name,'-',l1_name,'-',l2_name,'-',l3_name) as l3_name,
    concat(a.channel,'-',l0_name,'-',l1_name,'-',l2_name,'-',l3_name,'-',l4_name) as l4_name,
    concat(a.channel,'-',l0_name,'-',l1_name,'-',l2_name,'-',l3_name,'-',l4_name,'-',l5_name) as l5_name,
    concat(a.channel,'-',l0_name,'-',l1_name,'-',l2_name,'-',l3_name,'-',l4_name,'-',l5_name,'-',product_code) as product_code,
    a.*,
    c.wp,
    d.qty_2025,
    e.qty_2024,
    -- scenario_24,scenario_25,
    aur_2024,aur_2025
    from
    base as a
    left join
    prod_master as b
    on a.hierarchy_code = b.hierarchy_code
    left join
    wp_table as c
    on
    a.channel = c.channel
    and
    a.hierarchy_code = c.hierarchy_code
    and
    a.fiscal_year_week = c.fiscal_year_week
    and 
    a.scenario1 = c.scenario1
    left join
    ly_table as d
    on
    a.channel = d.channel
    and
    a.hierarchy_code = d.hierarchy_code
    and
    a.fiscal_year_week = d.fiscal_year_week
    left join
    lly_table as e
    on
    a.channel = e.channel
    and
    a.hierarchy_code = e.hierarchy_code
    and
    a.fiscal_year_week = e.fiscal_year_week
),

forecast as (
  select product_code,
    cast(left(cast(fiscal_year_week as string),4) as int64)  as fiscal_year,
    cast(right(cast(fiscal_year_week as string),2)  as int64)  as  fiscal_week,
    fiscal_year_week,
    round(percent_off,2) as scenario1,
    w_sls_u as qty
    from 
    balsambrands-20022025.balsam_ingestion_dev.balsam_itemsmart_forecast_level
),

fmt_2025 as(
select product_code,
       fiscal_year,
       fiscal_week,
       fiscal_year_week+100 as fiscal_year_week,
       qty as qty2025,
       oh,
       msrp,
       case when qty > 0 then 1 else 0 end as prod_with_sales
       from ai-ps6060.balsam_brands_ada.balsam_fmt_product_code_prod
       where 
      --  product_code in (select distinct product_code 
      --  from ai-ps6060.balsam_brands_ada.simulation_output_balsam_all__2025_12_09 )
      --  and 
       fiscal_year = 2025
       group by all),
fmt_2024 as(
select product_code,
       fiscal_year,
       fiscal_week,
       fiscal_year_week+100 as fiscal_year_week,
       qty as qty2024,
       oh,
       msrp,
       case when qty > 0 then 1 else 0 end as prod_with_sales
       from ai-ps6060.balsam_brands_ada.balsam_fmt_product_code_prod
       where 
      --  product_code in (select distinct product_code 
      --  from ai-ps6060.balsam_brands_ada.simulation_output_balsam_all__2025_12_09 )
      --  and 
       fiscal_year = 2024
       group by all),

comp_flag_table_30 as (
  with sales_summary AS (
    select a.*, b.most_recent_oh
    from
    (
      SELECT
          product_code,
          SUM(qty2025) AS total_units_2025,
          SUM(prod_with_sales) AS weeks_with_sales_2025,
          
      FROM
          fmt_2025
       where qty2025 > 0 
      GROUP BY
          product_code) as a

          left join

          (select product_code,
          -- Get the most recent On-Hand inventory for the product (using a window function)
          LAST_VALUE(oh) OVER (PARTITION BY product_code ORDER BY fiscal_year_week DESC) AS most_recent_oh
          from fmt_2025 )as b
          using(product_code) 
  ),
  sales_summary_2024 AS (
      SELECT
          product_code,
          SUM(qty2024) AS total_units_2024,
          SUM(prod_with_sales) AS weeks_with_sales_2024,
          COUNT(DISTINCT fiscal_week) AS total_weeks_2024
      FROM
          fmt_2024
      where qty2024 > 0
      GROUP BY
          product_code
  ),
  -- Step 2: Combine and Apply Frequency Criteria
  comp_candidates AS (
      SELECT
          t25.product_code,
          t25.total_units_2025,
          t24.total_units_2024,
          t25.most_recent_oh,
          t25.weeks_with_sales_2025,
          t24.weeks_with_sales_2024,
          t24.total_weeks_2024,
          -- Assuming 52 weeks in a fiscal year for calculation, or using the actual count from 2024
          t25.weeks_with_sales_2025 / t24.total_weeks_2024 AS sales_freq_2025,
          t24.weeks_with_sales_2024 / t24.total_weeks_2024 AS sales_freq_2024
      FROM
          sales_summary t25
      INNER JOIN
          sales_summary_2024 t24
          ON t25.product_code = t24.product_code
      )
      -- Step 3: Apply All Comparability Criteria
      SELECT
      distinct 
          product_code,
          -- total_units_2024,
          -- total_units_2025,
          -- most_recent_oh,
          -- sales_freq_2024,
          -- sales_freq_2025
      FROM
          comp_candidates
      WHERE
          -- CRITERION 1: Products sold in at least 30% of the weeks in both years
          sales_freq_2024 >= 0.30
          AND sales_freq_2025 >= 0.30

          -- CRITERION 2: LY and TY units are comparable (within 30%)
          -- This checks if the difference between TY and LY is no more than 30% of LY (or TY)
          -- $|TY - LY| / LY <= 0.30$  which is equivalent to $0.7 * LY <= TY <= 1.3 * LY$
          AND total_units_2025 BETWEEN total_units_2024 * 0.7 AND total_units_2024 * 1.3

          -- CRITERION 3: Has sufficient total inventory as of recent week
          -- This is a subjective criterion, assuming "sufficient" means inventory > 0 or a high threshold (e.g., > 100)
          -- Adjust the '100' threshold as needed for your business definition of 'sufficient'
          AND most_recent_oh > 100 
      ORDER BY
          product_code

      ),

comp_flag_table_50 as (
  with sales_summary AS (
    select a.*, b.most_recent_oh
    from
    (
      SELECT
          product_code,
          SUM(qty2025) AS total_units_2025,
          SUM(prod_with_sales) AS weeks_with_sales_2025,
          
      FROM
          fmt_2025
       where qty2025 > 0 
      GROUP BY
          product_code) as a

          left join

          (select product_code,
          -- Get the most recent On-Hand inventory for the product (using a window function)
          LAST_VALUE(oh) OVER (PARTITION BY product_code ORDER BY fiscal_year_week DESC) AS most_recent_oh
          from fmt_2025 )as b
          using(product_code) 
  ),
  sales_summary_2024 AS (
      SELECT
          product_code,
          SUM(qty2024) AS total_units_2024,
          SUM(prod_with_sales) AS weeks_with_sales_2024,
          COUNT(DISTINCT fiscal_week) AS total_weeks_2024
      FROM
          fmt_2024
      where qty2024 > 0
      GROUP BY
          product_code
  ),
  -- Step 2: Combine and Apply Frequency Criteria
  comp_candidates AS (
      SELECT
          t25.product_code,
          t25.total_units_2025,
          t24.total_units_2024,
          t25.most_recent_oh,
          t25.weeks_with_sales_2025,
          t24.weeks_with_sales_2024,
          t24.total_weeks_2024,
          -- Assuming 52 weeks in a fiscal year for calculation, or using the actual count from 2024
          t25.weeks_with_sales_2025 / t24.total_weeks_2024 AS sales_freq_2025,
          t24.weeks_with_sales_2024 / t24.total_weeks_2024 AS sales_freq_2024
      FROM
          sales_summary t25
      INNER JOIN
          sales_summary_2024 t24
          ON t25.product_code = t24.product_code
      )
      -- Step 3: Apply All Comparability Criteria
      SELECT
      distinct 
          product_code,
          -- total_units_2024,
          -- total_units_2025,
          -- most_recent_oh,
          -- sales_freq_2024,
          -- sales_freq_2025
      FROM
          comp_candidates
      WHERE
          -- CRITERION 1: Products sold in at least 30% of the weeks in both years
          sales_freq_2024 >= 0.30
          AND sales_freq_2025 >= 0.30

          -- CRITERION 2: LY and TY units are comparable (within 30%)
          -- This checks if the difference between TY and LY is no more than 30% of LY (or TY)
          -- $|TY - LY| / LY <= 0.30$  which is equivalent to $0.7 * LY <= TY <= 1.3 * LY$
          AND total_units_2025 BETWEEN total_units_2024 * 0.5 AND total_units_2024 * 1.5

          -- CRITERION 3: Has sufficient total inventory as of recent week
          -- This is a subjective criterion, assuming "sufficient" means inventory > 0 or a high threshold (e.g., > 100)
          -- Adjust the '100' threshold as needed for your business definition of 'sufficient'
          AND most_recent_oh > 100 
      ORDER BY
          product_code

      ),

    final as (
    select 
    a.*,
    SUBSTR(CAST(a.fiscal_year_week AS STRING), 1, 4) AS fiscal_year,
    SUBSTR(CAST(a.fiscal_year_week AS STRING), 5, 2) AS fiscal_week,
    b.qty as predicted
    from
    prod_table as a
    left join
    forecast as b
    using(product_code, fiscal_year_week, scenario1)
)
select 
*,
case when product_code in (select distinct product_code from comp_flag_table_30)then 1 else 0 end as comp_flag_30,
case when product_code in (select distinct product_code from comp_flag_table_50)then 1 else 0 end as comp_flag_50,
case when wp< 5 then 1 else 0 end as wp_5_flag,
case when predicted is null then 0 else 1 end as predicted_flag,
case when qty_2025 is not null then 1 else 0 end as sold_week_2025,
case when product_code in (select distinct product_code from fmt_2025 where qty2025 > 0) then 1 else 0 end as product_with_sales,
case when product_code in (select distinct product_code from ai-ps6060.balsam_brands_ada.simulation_output_balsam_all__2025_12_09_bias_corr_elasticity_base_others
where algorithm_name in ('oh_contribution_new_prod_avg','oh_contribution_new_prod'))
then 1 else 0 end as new_prod_flag
from
final
-- where predicted is not null
where fiscal_year_week <= 202643
group by all
  )),

commp_skus as (
        with fmt_2025 as(
    select product_code,
          fiscal_year,
          fiscal_week,
          fiscal_year_week+100 as fiscal_year_week,
          qty as qty2025,
          oh,
          msrp,
          case when qty > 0 then 1 else 0 end as prod_with_sales
          from `ai-ps6060.balsam_brands_ada.balsam_fmt_product_code_prod`
          where 
          --  product_code in (select distinct product_code 
          --  from ai-ps6060.balsam_brands_ada.simulation_output_balsam_all__2025_12_09 )
          --  and 
          fiscal_year = 2025
          group by all),
    fmt_2024 as (
    select product_code,
          fiscal_year,
          fiscal_week,
          fiscal_year_week+100 as fiscal_year_week,
          qty as qty2024,
          oh,
          msrp,
          case when qty > 0 then 1 else 0 end as prod_with_sales
          from ai-ps6060.balsam_brands_ada.balsam_fmt_product_code_prod
          where 
          --  product_code in (select distinct product_code 
          --  from ai-ps6060.balsam_brands_ada.simulation_output_balsam_all__2025_12_09 )
          --  and 
          fiscal_year = 2024
          group by all),
    comp_flag_table_50 as (
      with sales_summary AS (
        select a.*, b.most_recent_oh
        from
        (
          SELECT
              product_code,
              SUM(qty2025) AS total_units_2025,
              SUM(prod_with_sales) AS weeks_with_sales_2025,
              
          FROM
              fmt_2025
          where qty2025 > 0 
          GROUP BY
              product_code) as a
              left join
              (select product_code,
              -- Get the most recent On-Hand inventory for the product (using a window function)
              LAST_VALUE(oh) OVER (PARTITION BY product_code ORDER BY fiscal_year_week DESC) AS most_recent_oh
              from fmt_2025 )as b
              using(product_code) 
      ),
      sales_summary_2024 AS (
          SELECT
              product_code,
              SUM(qty2024) AS total_units_2024,
              SUM(prod_with_sales) AS weeks_with_sales_2024,
              COUNT(DISTINCT fiscal_week) AS total_weeks_2024
          FROM
              fmt_2024
          where qty2024 > 0
          GROUP BY
              product_code
      ),
      -- Step 2: Combine and Apply Frequency Criteria
      comp_candidates AS (
          SELECT
              t25.product_code,
              t25.total_units_2025,
              t24.total_units_2024,
              t25.most_recent_oh,
              t25.weeks_with_sales_2025,
              t24.weeks_with_sales_2024,
              t24.total_weeks_2024,
              -- Assuming 52 weeks in a fiscal year for calculation, or using the actual count from 2024
              t25.weeks_with_sales_2025 / t24.total_weeks_2024 AS sales_freq_2025,
              t24.weeks_with_sales_2024 / t24.total_weeks_2024 AS sales_freq_2024
          FROM
              sales_summary t25
          INNER JOIN
              sales_summary_2024 t24
              ON t25.product_code = t24.product_code
          )
          -- Step 3: Apply All Comparability Criteria
          SELECT
          distinct 
              product_code,
              -- total_units_2024,
              -- total_units_2025,
              -- most_recent_oh,
              -- sales_freq_2024,
              -- sales_freq_2025
          FROM
              comp_candidates
          WHERE
              -- CRITERION 1: Products sold in at least 30% of the weeks in both years
              sales_freq_2024 >= 0.30
              AND sales_freq_2025 >= 0.30
              -- CRITERION 2: LY and TY units are comparable (within 30%)
              -- This checks if the difference between TY and LY is no more than 30% of LY (or TY)
              -- $|TY - LY| / LY <= 0.30$  which is equivalent to $0.7 * LY <= TY <= 1.3 * LY$
              AND total_units_2025 BETWEEN total_units_2024 * 0.5 AND total_units_2024 * 1.5
              -- CRITERION 3: Has sufficient total inventory as of recent week
              -- This is a subjective criterion, assuming "sufficient" means inventory > 0 or a high threshold (e.g., > 100)
              -- Adjust the '100' threshold as needed for your business definition of 'sufficient'
              AND most_recent_oh > 100 
          ORDER BY
              product_code
          ),
    brand_sku as (
        select product_code,
        CASE 
            WHEN product_code LIKE 'Retail%' THEN SPLIT(product_code, '-')[2]
            ELSE SPLIT(product_code, '-')[1]
        END AS l0_name,
        CASE 
            WHEN product_code LIKE 'Retail%' THEN SPLIT(product_code, '-')[1]
            ELSE SPLIT(product_code, '-')[0]
        END AS channel,
        ARRAY_REVERSE(SPLIT(product_code, '-'))[SAFE_OFFSET(0)] AS sku
        from comp_flag_table_50
    )
    select  l0_name,sku,channel from brand_sku 
    -- channel can be adjusted as per requirement
    WHERE channel = 'Hybris'
  )

,oo as (
  
WITH
    po AS (
    SELECT
    DISTINCT
      CASE
        WHEN po.l0_name IN ('BHUS', 'BHUK', 'BHAU', 'BHDE', 'BHFR', 'BHCA','BHEU') THEN po.l0_name
        ELSE 'BHUS'
    END
      AS l0_name,
      po.sku AS sku,
      CAST(po_date AS DATE) AS po_date,
      date_shipped,
      quantity_shipped,
      cast(wr.date AS DATE) AS warehouse_date,
      ordered_quantity,
      po.channel AS channel,
      case when intial_eta <= current_date() + 1 and (date_shipped is null or date is null) then current_date() + 1 else
      po.intial_eta end as initial_eta
    FROM
      balsambrands-20022025.balsam_ingestion_prod.purchase_order_master AS po
    LEFT JOIN
      balsambrands-20022025.balsam_ingestion_prod.packing_list_master AS pl
    ON
      po.po_number = pl.po_number
      AND po.sku = pl.sku
    LEFT JOIN (
      SELECT
        * EXCEPT(product_code,
          date_received),
        product_code AS sku,
        date_received AS date
      FROM
        balsambrands-20022025.balsam_ingestion_prod.warehouse_receipt_master ) AS wr
    ON
      pl.po_number = wr.po_number
      AND pl.sku = wr.sku
      AND pl.container_number = wr.container_number
      AND pl.inventory_packing_list_summary_id = wr.inventory_packing_list_summary_id ),
oo_cal as
(
    SELECT
      DISTINCT 
      po.l0_name,
      cal1.date,
      cal1.fy_week_id,
      po.sku,
      po.channel AS channel,
      SUM(CASE
          WHEN po.po_date is not null AND po.date_shipped IS NULL AND po.warehouse_date IS NULL THEN po.ordered_quantity
          ELSE 0
      END
        )AS oo_temp,
      SUM(CASE
          WHEN po.po_date is not null AND po.date_shipped is not null AND po.warehouse_date IS NULL THEN po.quantity_shipped
          ELSE 0
      END
        )AS oo_temp_it
    FROM
      balsambrands-20022025.balsam_ingestion_prod.fiscal_calendar_master cal
    LEFT JOIN
      po
    ON
      po.po_date = cal.date
    left join 
      balsambrands-20022025.balsam_ingestion_prod.fiscal_calendar_master cal1
    ON
      po.initial_eta = cal1.date
    WHERE
      po.l0_name IS NOT NULL
      AND cal1.fy_week_id IS NOT NULL
      AND po.l0_name is not null and po.sku is not null
      AND po.initial_eta > current_date()
    GROUP BY
      1,
      2,
      3,
      4,
      5
      order by date asc
)

select l0_name, sku, channel, sum(oo_temp + oo_temp_it) as units from oo_cal
where channel='Ecom'
and fy_week_id=202601
group by 1,2,3
)


,oh as (
  with fiscal_date_info as (
  select
  distinct date
  from balsambrands-20022025.balsam_ingestion_prod.fiscal_date_mapping 
  where date= DATE_TRUNC(CURRENT_DATE(), WEEK(SATURDAY))

  )

  select
  im.l0_name
  ,im.sku
  ,im.platform
  ,im.oh as units
  from balsambrands-20022025.balsam_ingestion_prod.inventory_master im 
  join  fiscal_date_info as fd
  on fd.date=cast(im.inventorydate as date)
  where platform='Hybris'
)





,forecast AS (
  with forecast_before as(
    SELECT
    x.brand,
    x.sku,
    -- y.l1_name,
    -- y.l2_name,
    -- y.l3_name,
    -- y.l4_name,
    -- y.l5_name,
    hierarchy_code,
    channel,
    sub_channel,
    fiscal_year_week,
    percent_off,
    w_sls_u
  FROM `balsambrands-20022025.balsam_ingestion_prod.iaf_intermediate_itemsmart_temp` x
  JOIN (
      SELECT a.*
      FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
      JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
        USING (l0_name, sku)
      WHERE a.active = TRUE
        AND b.active = TRUE
        AND level = 7
  ) y
  USING (hierarchy_code)
  WHERE fiscal_year_week BETWEEN 202601 AND 202604
    AND sub_channel = 'Hybris'
)

  SELECT
    fb.brand as l0_name,
    fb.sku,
    -- fb.l1_name,
    -- fb.l2_name,
    -- fb.l3_name,
    -- fb.l4_name,
    -- fb.l5_name,
    fb.hierarchy_code,
    fb.channel,
    fb.sub_channel,
    fb.fiscal_year_week,
    CASE
      WHEN CONCAT(brand, sku) IN (
             SELECT DISTINCT CONCAT(l0_name, sku)
             FROM commp_skus
           )
      THEN 1 ELSE 0
    END AS comp_flag,
    dbv.percent_off AS percent_off,
    fb.w_sls_u
  FROM forecast_before fb
  JOIN dbv
    ON fb.hierarchy_code = dbv.hierarchy_code
   AND fb.percent_off = dbv.percent_off 
   and fb.fiscal_year_week = dbv.fiscal_year_week
   where fb.percent_off = dbv.percent_off

   union all

    SELECT
    fb.brand as l0_name,
    fb.sku,
    -- fb.l1_name,
    -- fb.l2_name,
    -- fb.l3_name,
    -- fb.l4_name,
    -- fb.l5_name,
    fb.hierarchy_code,
    fb.channel,
    fb.sub_channel,
    fb.fiscal_year_week,
    CASE
      WHEN CONCAT(brand, sku) IN (
             SELECT DISTINCT CONCAT(l0_name, sku)
             FROM commp_skus
           )
      THEN 1 ELSE 0
    END AS comp_flag,
    0.3 AS percent_off,
    fb.w_sls_u
  FROM forecast_before fb
  LEFT JOIN dbv
    ON fb.hierarchy_code = dbv.hierarchy_code
   AND fb.percent_off = dbv.percent_off
   and fb.fiscal_year_week = dbv.fiscal_year_week
   where fb.hierarchy_code not in (select distinct hierarchy_code from dbv) and fb.percent_off = 0.3
)


-- select * from forecast
-- where l0_name = 'BHUS' and sku = '4004031'


,ty as (
      SELECT *,
            CASE
      WHEN CONCAT(l0_name, sku) IN (
             SELECT DISTINCT CONCAT(l0_name, sku)
             FROM commp_skus
           )
      THEN 1 ELSE 0 end as comp_flag,
    --   b.hierarchy_code,
FROM `balsambrands-20022025.balsam_ingestion_prod.itemsmart_tool_master` itm
-- left JOIN (
--     SELECT a.*
--     FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
--     JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
--       USING (l0_name, sku)
--     WHERE a.active = TRUE
--       AND b.active = TRUE
--       AND level = 7
-- ) b
-- USING (l0_name, sku)
WHERE itm.fy_week_id BETWEEN 202549 AND 202552
  AND itm.sub_channel = 'Hybris'
  -- and itm.units > 0
)


,ly_for_fct as (
      SELECT *,
            CASE
      WHEN CONCAT(l0_name, sku) IN (
             SELECT DISTINCT CONCAT(l0_name, sku)
             FROM commp_skus
           )
      THEN 1 ELSE 0 end as comp_flag,
      -- b.hierarchy_code,
FROM `balsambrands-20022025.balsam_ingestion_prod.itemsmart_tool_master` itm
-- left JOIN (
--     SELECT a.*
--     FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
--     JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
--       USING (l0_name, sku)
--     WHERE a.active = TRUE
--       AND b.active = TRUE
--       AND level = 7
-- ) b
-- USING (l0_name, sku)
WHERE itm.fy_week_id BETWEEN 202501 AND 202504
  AND itm.sub_channel = 'Hybris'
  -- and itm.units > 0
)



,ly_for_ty as (
      SELECT *,
            CASE
      WHEN CONCAT(l0_name, sku) IN (
             SELECT DISTINCT CONCAT(l0_name, sku)
             FROM commp_skus
           )
      THEN 1 ELSE 0 end as comp_flag,
      -- b.hierarchy_code,
FROM `balsambrands-20022025.balsam_ingestion_prod.itemsmart_tool_master` itm
-- left JOIN (
--     SELECT a.*
--     FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
--     JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
--       USING (l0_name, sku)
--     WHERE a.active = TRUE
--       AND b.active = TRUE
--       AND level = 7
-- ) b
-- USING (l0_name, sku)
WHERE itm.fy_week_id BETWEEN 202449 AND 202452
  AND itm.sub_channel = 'Hybris'
  -- and itm.units > 0
)

-- select * from ly_for_ty

,agg_ly_for_fct as (
  select l0_name,sku, sum(units) as units
  from ly_for_fct lf  JOIN (
      SELECT a.*
      FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
      JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
        USING (l0_name, sku)
      WHERE a.active = TRUE
        AND b.active = TRUE
        AND level = 7
  ) b
  USING (l0_name,sku)
  where comp_flag = 1
  group by 1,2
)


,agg_ly_for_ty as (
  select l0_name,sku, sum(units) as units
  from ly_for_ty lt  JOIN (
      SELECT a.*
      FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
      JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
        USING (l0_name, sku)
      WHERE a.active = TRUE
        AND b.active = TRUE
        AND level = 7
  ) b
  USING (l0_name,sku)
  where comp_flag = 1
  group by 1,2
)

,agg_ty as (
  select l0_name,sku, sum(units) as units
  from ty ty  JOIN (
      SELECT a.*
      FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
      JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
        USING (l0_name, sku)
      WHERE a.active = TRUE
        AND b.active = TRUE
        AND level = 7
  ) b
  USING (l0_name,sku)
  where comp_flag = 1
  group by 1,2
)

,agg_fct as (
  select l0_name, sku, sum(w_sls_u) as units
  from forecast fct 
  -- JOIN (
  --     SELECT a.*
  --     FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
  --     JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
  --       USING (l0_name, sku)
  --     WHERE a.active = TRUE
  --       AND b.active = TRUE
  --       AND level = 7
  -- ) b
  -- USING (hierarchy_code)
  where comp_flag = 1
  group by 1,2
)

-- select * from agg_fct


-- SELECT distinct l0_name,sku from agg_ty


-- select * from agg_fct where l0_name  = 'BHUS' and l1_name = 'Christmas Decorations' 
, final AS (
  SELECT 
    fct.l0_name,
    fct.sku,

    COALESCE(fct.units, 0) AS fct_units,
    COALESCE(ty.units, 0)  AS ty_units,
    COALESCE(lt.units, 0)  AS lt_units,
    COALESCE(lf.units, 0)  AS lf_units,

    SAFE_DIVIDE(COALESCE(ty.units, 0), COALESCE(lt.units, 0)) AS ty_ly_units_ratio,
    SAFE_DIVIDE(COALESCE(fct.units, 0), COALESCE(lf.units, 0)) AS fct_ly_units_ratio,

    CASE
      WHEN
        COALESCE(lt.units, 0) > 0
        AND COALESCE(lf.units, 0) > 0
        AND ABS(
          COALESCE(SAFE_DIVIDE(COALESCE(ty.units, 0), COALESCE(lt.units, 0)),0) -
          COALESCE(SAFE_DIVIDE(COALESCE(fct.units, 0), COALESCE(lf.units, 0)),0)
        ) <= 0.5
      THEN TRUE
      ELSE FALSE
    END AS explainable,

    CASE
      WHEN
        COALESCE(lt.units, 0) > 0
        AND COALESCE(lf.units, 0) > 0
        AND ABS(
          COALESCE(SAFE_DIVIDE(COALESCE(ty.units, 0), COALESCE(lt.units, 0)),0) -
          COALESCE(SAFE_DIVIDE(COALESCE(fct.units, 0), COALESCE(lf.units, 0)),0)
        ) <= 0.5
      THEN FALSE
      ELSE TRUE
    END AS non_explainable,

    oh.units as oh_units,
    oo.units as oo_units,

  FROM agg_fct fct
  LEFT JOIN agg_ly_for_fct lf
    ON fct.l0_name = lf.l0_name AND fct.sku = lf.sku 
  LEFT JOIN agg_ly_for_ty lt
    ON fct.l0_name = lt.l0_name AND fct.sku = lt.sku
  LEFT JOIN agg_ty ty
    ON fct.l0_name = ty.l0_name AND fct.sku = ty.sku
  left join oh oh
    on fct.l0_name = oh.l0_name and fct.sku = oh.sku
  left join oo oo
    on fct.l0_name = oh.l0_name and fct.sku = oo.sku
)

-- SELECT 
-- -- f.l0_name,b.l1_name,b.l2_name,b.l3_name,f.sku,f.* EXCEPT (l0_name, sku)

--     f.l0_name  as  `Brand`,
--     b.l1_name  as  `Department`,
--     b.l2_name  as  `Sub Department`,
--     b.l3_name  as  `Class`,
--     f.sku  as  `SKU`,
--     f.ty_units  as  `ForecastRecent4Week`,
--     f.lf_units  as  `LY4Week`,
--     f.fct_units  as  `Forecast4Week`,
--     f.lt_units  as  `LYRecent4Week`,
--     ty_ly_units_ratio  as  `ForecastRecent4week_to_LYRecent4week`,
--     fct_ly_units_ratio  as  `Forecast4week_to_LY4week`,
--     concat(f.l0_name,f.sku)  as  `concat`,
--     f.oh_units  as  `End of Period Inv`,
--     f.oo_units  as  `On Order`,
--     f.explainable  as  `Explainability from 1a`

-- FROM final f 
-- JOIN (
--       SELECT a.*
--       FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
--       JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
--         USING (l0_name, sku)
--       WHERE a.active = TRUE
--         AND b.active = TRUE
--         AND level = 7
--   ) b
-- USING (l0_name,sku)

-- select * from final


SELECT
    l0_name,
    b.l1_name,
    COUNT(CASE WHEN explainable = true THEN sku END) AS explainable_count,
    COUNT(CASE WHEN non_explainable = true THEN sku END) AS non_explainable_count
FROM final
  JOIN (
      SELECT a.*
      FROM `balsambrands-20022025.balsam_ingestion_prod.product_hierarchies_filter_flattened` a
      JOIN `balsambrands-20022025.balsam_ingestion_prod.product_master` b
        USING (l0_name, sku)
      WHERE a.active = TRUE
        AND b.active = TRUE
        AND level = 7
  ) b
  USING (l0_name,sku)
GROUP BY l0_name, b.l1_name

