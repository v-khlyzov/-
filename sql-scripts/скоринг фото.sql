WITH base_data AS (
    SELECT
        CASE
            WHEN (((vp.attributes::jsonb) -> 'applicationId') ->> 0) = '1p' THEN '1P'
            WHEN (((vp.attributes::jsonb) -> 'applicationId') ->> 0) = '3p' THEN '3P'
            ELSE (((vp.attributes::jsonb) -> 'applicationId') ->> 0)
        END AS item_type,
        vp.product_id,
        (((vp.attributes::jsonb) -> 'departmentID') ->> 0)::INT AS division_id,
        (((vp.attributes::jsonb) -> 'subDepartmentID') ->> 0)::INT AS subdivision_id,
        jsonb_array_length((vp.attributes::jsonb) -> 'mediaPdpPhoto') AS pdp_photo_count,
        LEAST(jsonb_array_length((vp.attributes::jsonb) -> 'mediaPdpPhoto'), 8) AS content_score
    FROM datalake_ods.v_products AS vp
    WHERE vp.is_actual = '1'
        AND vp.product_id ~ '^\d+$'
        AND jsonb_array_length((vp.attributes::jsonb) -> 'mediaPdpPhoto') IS NOT NULL
),
avg_content_score AS (
    SELECT
        subdivision_id,
        item_type,
        AVG(content_score) AS avg_content_score
    FROM base_data
    GROUP BY subdivision_id, item_type
)
SELECT
	CONCAT(bd.item_type, '-', bd.subdivision_id) as key,
    bd.item_type,
    bd.division_id,
    bd.subdivision_id,
    acs.avg_content_score
FROM base_data bd
JOIN avg_content_score acs ON bd.subdivision_id = acs.subdivision_id AND bd.item_type = acs.item_type
GROUP BY CONCAT(bd.item_type, '-', bd.subdivision_id), bd.item_type, bd.division_id, bd.subdivision_id, acs.avg_content_score
ORDER BY bd.division_id, bd.subdivision_id, bd.item_type;
