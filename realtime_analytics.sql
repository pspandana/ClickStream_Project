-- Real-time Active Users (Last 5 minutes)
SELECT 
    COUNT(DISTINCT user_id) as active_users,
    MIN(timestamp) as earliest_event,
    MAX(timestamp) as latest_event
FROM `clickstream_analytics.clicks_raw`
WHERE TIMESTAMP(timestamp, 'America/Chicago') >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);

-- Purchase Analysis (Last 15 minutes)
SELECT 
    FORMAT_TIMESTAMP('%H:%M', TIMESTAMP(timestamp, 'America/Chicago')) as minute,
    COUNT(*) as purchases,
    ROUND(SUM(CAST(price as FLOAT64)), 2) as revenue_usd,
    COUNT(DISTINCT user_id) as unique_buyers
FROM `clickstream_analytics.clicks_raw`
WHERE event_type = 'purchase'
AND TIMESTAMP(timestamp, 'America/Chicago') >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY minute
ORDER BY minute DESC;

-- Popular Products Analysis
SELECT 
    category,
    product_id,
    COUNT(*) as view_count,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as cart_adds,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    ROUND(AVG(CAST(price as FLOAT64)), 2) as avg_price_usd
FROM `clickstream_analytics.clicks_raw`
WHERE TIMESTAMP(timestamp, 'America/Chicago') >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY category, product_id
ORDER BY view_count DESC
LIMIT 10;

-- Error Monitoring
SELECT 
    http_status_code,
    error_type,
    error_message,
    COUNT(*) as error_count,
    ROUND(AVG(CAST(response_time_ms as INT64)), 2) as avg_response_time_ms,
    COUNT(DISTINCT user_id) as affected_users
FROM `clickstream_analytics.clicks_raw`
WHERE TIMESTAMP(timestamp, 'America/Chicago') >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
AND (http_status_code != '200' OR error_type IS NOT NULL)
GROUP BY http_status_code, error_type, error_message
ORDER BY error_count DESC;

-- Cart Abandonment Analysis
WITH user_actions AS (
    SELECT 
        user_id,
        session_id,
        MIN(CASE WHEN event_type = 'add_to_cart' THEN timestamp END) as first_cart_add,
        MAX(CASE WHEN event_type = 'purchase' THEN timestamp END) as last_purchase,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as cart_adds,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
        SUM(CASE WHEN event_type = 'purchase' THEN CAST(price as FLOAT64) ELSE 0 END) as revenue
    FROM `clickstream_analytics.clicks_raw`
    WHERE TIMESTAMP(timestamp, 'America/Chicago') >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
    GROUP BY user_id, session_id
)
SELECT 
    COUNT(DISTINCT CASE WHEN cart_adds > 0 THEN user_id END) as users_with_cart,
    COUNT(DISTINCT CASE WHEN purchases > 0 THEN user_id END) as users_with_purchase,
    COUNT(DISTINCT CASE WHEN cart_adds > 0 AND purchases = 0 THEN user_id END) as users_abandoned,
    ROUND(COUNT(DISTINCT CASE WHEN cart_adds > 0 AND purchases = 0 THEN user_id END) * 100.0 / 
          COUNT(DISTINCT CASE WHEN cart_adds > 0 THEN user_id END), 2) as abandonment_rate,
    ROUND(AVG(CASE WHEN purchases > 0 THEN revenue END), 2) as avg_purchase_value
FROM user_actions;

-- User Journey Analysis
SELECT 
    event_type,
    url,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions,
    ROUND(AVG(CAST(response_time_ms as INT64)), 2) as avg_response_time_ms
FROM `clickstream_analytics.clicks_raw`
WHERE TIMESTAMP(timestamp, 'America/Chicago') >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY event_type, url
ORDER BY event_count DESC
LIMIT 10;
