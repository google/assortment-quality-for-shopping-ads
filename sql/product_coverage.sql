CREATE OR REPLACE VIEW {datasetId}.product_coverage AS (
  WITH best_sellers AS (
    SELECT DISTINCT rank_timestamp,
      rank,
      rank_id,
      previous_rank,
      ranking_country,
      ranking_category,
      brand,
      google_brand_id,
      google_product_category,
      rcp.name as category_name,
      pt.name as product_name,
      price_range.min as price_range_min,
      price_range.max as price_range_max,
      price_range.currency as price_range_currency,
      _PARTITIONDATE as PARTITIONDATE
    FROM `{datasetId}.BestSellers_TopProducts_{gmcId}`  as tp,
    tp.ranking_category_path as rcp,
    tp.product_title as pt
    WHERE
      DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND ranking_country IN ('FR')
      AND rcp.locale IN ('fr-FR')
      AND pt.locale IN ('fr-FR', null, '')
      AND rank_id LIKE '%:FR:%'
  ),
  inventory AS (
    SELECT DISTINCT rank_id, product_id, merchant_id, aggregator_id
    FROM `{datasetId}.BestSellers_TopProducts_Inventory_{gmcId}`
    WHERE
      DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),':FR:%'))
      AND product_id LIKE '%:FR:%'
  )

  SELECT DISTINCT rank_timestamp,
      rank,
      bs.rank_id,
      previous_rank,
      ranking_country,
      ranking_category,
      brand,
      google_brand_id,
      google_product_category,
      category_name,
      product_name,
      price_range_min,
      price_range_max,
      price_range_currency,
      PARTITIONDATE,
      merchant_id,
      aggregator_id
      product_id,
      LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
      CASE WHEN
       product_id IS NULL THEN 'MISSING'
       ELSE 'AVAILABLE'
     END AS status
  FROM best_sellers AS bs
  LEFT JOIN inventory AS i
  ON bs.rank_id = i.rank_id
)