SELECT usage_date,
       billing_origin_product,
       sku_name,
       ROUND(SUM(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= DATE'2026-11-16'
GROUP BY ALL
ORDER BY usage_date, dbus DESC;

SELECT u.billing_origin_product,
       ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS approx_list_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_date >= DATE'2026-11-16'
GROUP BY ALL
ORDER BY approx_list_cost_usd DESC;
