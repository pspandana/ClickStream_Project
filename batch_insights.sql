-- 1. Daily Active Users
SELECT 
    DATE(timestamp) as date,
    COUNT(DISTINCT user_id) as daily_active_users
FROM `clickstream_analytics.clickstream_batch`
GROUP BY date
ORDER BY date;

-- 2. Top Product Categories by Views
SELECT 
    category,
    COUNT(*) as view_count
FROM `clickstream_analytics.clickstream_batch`
WHERE event_type = 'page_view'
GROUP BY category
ORDER BY view_count DESC
LIMIT 10;

-- 3. Conversion Rate by Category
SELECT 
    category,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) * 100.0 / 
    COUNT(DISTINCT session_id) as conversion_rate
FROM `clickstream_analytics.clickstream_batch`
GROUP BY category
ORDER BY conversion_rate DESC;

-- 4. Average Session Duration
SELECT 
    DATE(timestamp) as date,
    AVG(TIMESTAMP_DIFF(
        TIMESTAMP(session_end_time), 
        TIMESTAMP(session_start_time), 
        SECOND
    )) as avg_session_duration_seconds
FROM `clickstream_analytics.clickstream_batch`
GROUP BY date
ORDER BY date;

-- 5. Error Rate Analysis
SELECT 
    error_type,
    COUNT(*) as error_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as error_percentage
FROM `clickstream_analytics.clickstream_batch`
WHERE error_type IS NOT NULL AND error_type != ''
GROUP BY error_type
ORDER BY error_count DESC;
