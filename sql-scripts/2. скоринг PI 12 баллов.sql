WITH base_data AS (
    SELECT
        '1P' AS item_type,
        IF(department < 10, CONCAT('0', toString(department)), toString(department)) AS subdivision_id,
        product_id,
        region_code,
        price,
        ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1
        ) AS avg_competitor_price,
        if(price / ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1) IS NULL OR ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1) = 0, 1, price / ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1)
        ) AS price_index,
        ROW_NUMBER() OVER (PARTITION BY product_id, region_code ORDER BY `date` DESC) AS rn
    FROM main.pi_history_1p
    WHERE `date` BETWEEN toDate(today() - INTERVAL 14 DAY) AND toDate(today() - INTERVAL 1 DAY)
        AND product_id IS NOT NULL
        AND notEmpty(competitors.competitor_price) = 1
        AND region_code = 'RU-MOW'
        AND department <> '0'
    UNION ALL
    SELECT
        '3P' AS item_type,
        IF(department < 10, CONCAT('0', toString(department)), toString(department)) AS subdivision_id,
        product_id,
        region_code,
        price,
        ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1
        ) AS avg_competitor_price,
        if(price / ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1) IS NULL OR ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1) = 0, 1, price / ifNull(
            arrayAvg(
                arrayFilter(
                    (x1, x2) -> x1 > 0 AND x1 / price BETWEEN 0.10 AND 10.0 AND x2 = 1,
                    competitors.competitor_price,
                    competitors.full_analog
                )
            ), 1)
        ) AS price_index,
        ROW_NUMBER() OVER (PARTITION BY product_id, region_code ORDER BY `date` DESC) AS rn
    FROM main.pi_history_3p
    WHERE `date` BETWEEN toDate(today() - INTERVAL 14 DAY) AND toDate(today() - INTERVAL 1 DAY)
        AND product_id IS NOT NULL
        AND notEmpty(competitors.competitor_price) = 1
        AND region_code = 'RU-MOW'
        AND department <> '0'
),
filtered_data AS (
    SELECT
        item_type,
        subdivision_id,
        product_id,
        price,
        avg_competitor_price,
        price_index,
        CASE
            WHEN price_index <= 0.95 THEN 12
            WHEN price_index >= 1.20 THEN 1
            ELSE 12 - 44 * (price_index - 0.95)
        END AS price_index_score
    FROM base_data
    WHERE rn = 1
),
avg_price_index_score AS (
    SELECT
        subdivision_id,
        item_type,
        AVG(price_index) AS avg_price_index,
        AVG(price_index_score) AS avg_price_index_score
    FROM filtered_data
    GROUP BY subdivision_id, item_type
)
SELECT
    CONCAT(fd.item_type, '-', fd.subdivision_id) AS key,
    ap.avg_price_index,
    ap.avg_price_index_score
FROM filtered_data fd
JOIN avg_price_index_score ap ON fd.subdivision_id = ap.subdivision_id AND fd.item_type = ap.item_type
GROUP BY CONCAT(fd.item_type, '-', fd.subdivision_id), ap.avg_price_index, ap.avg_price_index_score
ORDER BY CONCAT(fd.item_type, '-', fd.subdivision_id);