WITH base_data AS (
    SELECT
        CASE
            WHEN gamma <> 'm' THEN '1p'
            ELSE '3p'
        END AS product_type,
        toInt32OrNull(division_id) AS division_id,
        toInt32OrNull(subdivision_id) AS subdivision_id,
        product_id,
        SUM(impressions_in_catalogue) AS total_impressions_catalogue,
        SUM(adds_from_catalogue) AS total_adds_from_catalogue,
        SUM(details) AS total_details,
        SUM(adds_from_detail) AS total_adds_from_detail
    FROM bi.products
    WHERE report_date BETWEEN toDate(today() - INTERVAL 14 DAY) AND toDate(today() - INTERVAL 1 DAY)
        AND division_id IS NOT NULL
        AND subdivision_id IS NOT NULL
    GROUP BY product_type, division_id, subdivision_id, product_id
),
weighted_metrics AS (
    SELECT
        subdivision_id,
        SUM(total_details * (total_details / NULLIF(total_impressions_catalogue, 0))) / SUM(total_details) AS CR_PLP_to_PDP,
        SUM(total_details * (total_adds_from_catalogue / NULLIF(total_impressions_catalogue, 0))) / SUM(total_details) AS CR_PLP_A2C,
        SUM(total_details * (total_adds_from_detail / NULLIF(total_details, 0))) / SUM(total_details) AS CR_PDP_A2C
    FROM base_data
    GROUP BY subdivision_id
)
SELECT
  CONCAT(bd.product_type, '-', bd.subdivision_id) as key,
    bd.product_type,
    bd.division_id,
    bd.subdivision_id,
    wm.CR_PLP_to_PDP,
    wm.CR_PLP_A2C,
    wm.CR_PDP_A2C
FROM base_data bd
JOIN weighted_metrics wm ON bd.subdivision_id = wm.subdivision_id
GROUP BY CONCAT(bd.product_type, '-', bd.subdivision_id), bd.product_type, bd.division_id, bd.subdivision_id, wm.CR_PLP_to_PDP, wm.CR_PLP_A2C, wm.CR_PDP_A2C
ORDER BY bd.division_id, bd.subdivision_id, bd.product_type;

