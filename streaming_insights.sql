-- 1. Most Active Users
SELECT user_id, COUNT(*) as event_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 10;

-- 2. Popular Products by Event Type
SELECT product_id, event_type, COUNT(*) as event_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY product_id, event_type
ORDER BY event_count DESC
LIMIT 10;

-- 3. Average Response Time by URL
SELECT url, AVG(CAST(response_time_ms AS INT64)) as avg_response_time
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY url
ORDER BY avg_response_time DESC;

-- 4. Error Rate Analysis
SELECT http_status_code, COUNT(*) as error_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
WHERE http_status_code != '200'
GROUP BY http_status_code
ORDER BY error_count DESC;

-- 5. Product Category Performance
SELECT category, COUNT(*) as view_count,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchase_count,
  COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as cart_adds
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY category
ORDER BY view_count DESC;

-- 6. Session Duration Analysis
SELECT 
  session_id,
  TIMESTAMP_DIFF(TIMESTAMP(session_end_time), TIMESTAMP(session_start_time), MINUTE) as duration_minutes
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
ORDER BY duration_minutes DESC
LIMIT 10;

-- 7. User Journey Analysis
SELECT 
  user_id,
  STRING_AGG(event_type, ' -> ' ORDER BY TIMESTAMP(timestamp)) as user_journey
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY user_id
LIMIT 10;

-- 8. Peak Traffic Hours
SELECT 
  EXTRACT(HOUR FROM TIMESTAMP(timestamp)) as hour_of_day,
  COUNT(*) as event_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY hour_of_day
ORDER BY event_count DESC;

-- 9. Cart Abandonment Rate
WITH cart_actions AS (
  SELECT
    user_id,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as cart_adds,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
  FROM `click-stream-463520.clickstream_analytics.clicks_raw`
  GROUP BY user_id
)
SELECT 
  ROUND(100 * (1 - SUM(purchases)/SUM(cart_adds)), 2) as cart_abandonment_rate
FROM cart_actions;

-- 10. Product Price Range Analysis
SELECT
  CASE
    WHEN CAST(price AS FLOAT64) < 50 THEN 'Low (<$50)'
    WHEN CAST(price AS FLOAT64) < 100 THEN 'Medium ($50-$100)'
    ELSE 'High (>$100)'
  END as price_range,
  COUNT(*) as product_count,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchase_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY price_range
ORDER BY product_count DESC;

-- 11. Geographic Distribution
SELECT
  SPLIT(geolocation, ',')[OFFSET(1)] as country,
  COUNT(*) as visit_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY country
ORDER BY visit_count DESC
LIMIT 10;

-- 12. User Agent Analysis
SELECT
  CASE
    WHEN LOWER(user_agent) LIKE '%mobile%' THEN 'Mobile'
    WHEN LOWER(user_agent) LIKE '%tablet%' THEN 'Tablet'
    ELSE 'Desktop'
  END as device_type,
  COUNT(*) as visit_count
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY device_type
ORDER BY visit_count DESC;

-- 13. Conversion Rate by Category
SELECT
  category,
  COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END) as total_sessions,
  COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) as purchase_sessions,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) /
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END), 2) as conversion_rate
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY category;

-- 14. Error Analysis by URL
SELECT
  url,
  COUNT(*) as total_hits,
  COUNT(CASE WHEN http_status_code = '404' THEN 1 END) as not_found_errors,
  COUNT(CASE WHEN http_status_code = '500' THEN 1 END) as server_errors
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY url
ORDER BY total_hits DESC;

-- 15. Session Depth Analysis
SELECT
  session_id,
  COUNT(*) as actions_in_session,
  COUNT(DISTINCT url) as unique_pages_visited
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY session_id
ORDER BY actions_in_session DESC
LIMIT 10;

-- 16. Color Preference Analysis
SELECT
  colour,
  COUNT(*) as views,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
  ROUND(AVG(CAST(price AS FLOAT64)), 2) as avg_price
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY colour
ORDER BY views DESC;

-- 17. User Return Rate
SELECT
  user_id,
  COUNT(DISTINCT DATE(TIMESTAMP(timestamp))) as days_active,
  COUNT(*) as total_events
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY user_id
HAVING days_active > 1
ORDER BY days_active DESC
LIMIT 10;

-- 18. Product Viewing Time Analysis
SELECT
  product_id,
  AVG(CAST(response_time_ms AS INT64)) as avg_viewing_time,
  COUNT(*) as total_views
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
WHERE event_type = 'page_view'
GROUP BY product_id
HAVING total_views > 5
ORDER BY avg_viewing_time DESC;

-- 19. Entry Page Analysis
SELECT
  entry_url,
  COUNT(DISTINCT session_id) as session_starts,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as led_to_purchase
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
GROUP BY entry_url
ORDER BY session_starts DESC;

-- 20. Error Impact Analysis
SELECT
  error_type,
  COUNT(*) as error_count,
  COUNT(DISTINCT session_id) as affected_sessions,
  COUNT(DISTINCT user_id) as affected_users,
  ROUND(AVG(CAST(response_time_ms AS INT64)), 2) as avg_response_time
FROM `click-stream-463520.clickstream_analytics.clicks_raw`
WHERE error_type IS NOT NULL AND error_type != ''
GROUP BY error_type
ORDER BY error_count DESC;
