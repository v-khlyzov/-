WITH product_locations AS (
    SELECT DISTINCT
        product_id,
        location_id
    FROM 
        main.v_stock_snapshots
    WHERE 
        qty_availableforsale > 0
        AND snapshot_date::DATE BETWEEN CURRENT_DATE - INTERVAL '60 days' AND CURRENT_DATE - INTERVAL '1 day'
),
base_data AS (
    SELECT
        CASE
            WHEN (((vp.attributes::jsonb) -> 'applicationId') ->> 0) = '1p' THEN '1P'
            WHEN (((vp.attributes::jsonb) -> 'applicationId') ->> 0) = '3p' THEN '3P'
            ELSE (((vp.attributes::jsonb) -> 'applicationId') ->> 0)
        END AS item_type,
        (((vp.attributes::jsonb) -> 'subDepartmentID') ->> 0)::INT AS subdivision_id,
        product_id,
        AVG(available_days_count) AS total_available_days,
        AVG(available_days_frac_per_location) AS available_days_frac_per_location,
        SUM(CASE WHEN available_days_frac_per_location = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT location_id)::FLOAT AS full_availability_fraction,
		CASE
            WHEN AVG(available_days_count) <= 1 THEN 1
            WHEN AVG(available_days_count) <= 2 THEN 2
            WHEN AVG(available_days_count) <= 3 THEN 3
            WHEN AVG(available_days_count) <= 4 THEN 4
            WHEN AVG(available_days_count) <= 5 THEN 5
            WHEN AVG(available_days_count) <= 6 THEN 6
            WHEN AVG(available_days_count) <= 7 THEN 7
            WHEN AVG(available_days_count) <= 8 THEN 8
            WHEN AVG(available_days_count) <= 9 THEN 9
            WHEN AVG(available_days_count) <= 10 THEN 10
            WHEN AVG(available_days_count) <= 11 THEN 11
            ELSE 12
        END AS availability_score
    FROM
        (
            SELECT
                pl.product_id,
                pl.location_id,
                SUM(CASE WHEN qty_availableforsale > 0 THEN 1 END) AS available_days_count,
                SUM(CASE WHEN qty_availableforsale > 0 THEN 1 END) / COUNT(*)::FLOAT AS available_days_frac_per_location
            FROM 
                stockrepository_marts.v_stock_daily_snapshots AS sd
                JOIN dds.v_dict_stores AS ds
                    ON ds.store = sd.location_id::INT
                RIGHT JOIN product_locations AS pl
                    ON pl.product_id = sd.product_id
                    AND pl.location_id = sd.location_id
            WHERE
                snapshot_date::DATE BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE - INTERVAL '1 day'
                AND qty_availableforsale >= 0
            GROUP BY
                pl.product_id,
                pl.location_id
        ) AS b
    JOIN datalake.v_products AS vp
        ON b.product_id::TEXT = vp.product_id
        AND vp.is_actual = '1'
    GROUP BY item_type, subdivision_id, product_id
)
SELECT
	CONCAT(item_type, '-', subdivision_id) AS key,
	item_type,
    subdivision_id,
    AVG(availability_score) AS avg_availability_score
FROM base_data
GROUP BY CONCAT(item_type, '-', subdivision_id), item_type, subdivision_id
ORDER BY CONCAT(item_type, '-', subdivision_id)