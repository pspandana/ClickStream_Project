-- 1. Real-time Active Users (Last 5 minutes)
SELECT COUNT(DISTINCT user_id) as active_users
FROM `clickstream_analytics.clicks_raw`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), MINUTE) <= 5;

-- 2. Live Purchase Tracking
SELECT 
    TIMESTAMP_TRUNC(TIMESTAMP(timestamp), MINUTE) as minute,
    COUNT(*) as purchases,
    SUM(CAST(price as FLOAT64)) as revenue
FROM `clickstream_analytics.clicks_raw`
WHERE event_type = 'purchase'
AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), HOUR) <= 1
GROUP BY minute
ORDER BY minute DESC;

-- 3. Real-time Error Monitoring
SELECT 
    error_type,
    COUNT(*) as error_count,
    MAX(timestamp) as last_occurrence
FROM `clickstream_analytics.clicks_raw`
WHERE error_type IS NOT NULL 
AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), MINUTE) <= 15
GROUP BY error_type
ORDER BY error_count DESC;

-- 4. Popular Products (Last 30 minutes)
SELECT 
    product_id,
    category,
    COUNT(*) as view_count
FROM `clickstream_analytics.clicks_raw`
WHERE event_type = 'page_view'
AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), MINUTE) <= 30
GROUP BY product_id, category
ORDER BY view_count DESC
LIMIT 10;

-- 5. Cart Abandonment Rate
WITH cart_actions AS (
    SELECT 
        session_id,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchased
    FROM `clickstream_analytics.clicks_raw`
    WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), HOUR) <= 1
    GROUP BY session_id
)
SELECT 
    COUNT(CASE WHEN added_to_cart = 1 AND purchased = 0 THEN 1 END) * 100.0 / 
    COUNT(CASE WHEN added_to_cart = 1 THEN 1 END) as abandonment_rate
FROM cart_actions;
