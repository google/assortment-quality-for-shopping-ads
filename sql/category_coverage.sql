CREATE OR REPLACE VIEW {datasetId}.category_coverage AS (
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
        rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}`)
        AND ranking_country IN ('FR')
        AND rcp.locale IN ('fr-FR')
        AND pt.locale IN ('fr-FR', null, '')
        AND rank_id LIKE '%:FR:%'
  ),
  inventory AS (
    SELECT DISTINCT rank_id, product_id, merchant_id, aggregator_id
    FROM `merchant_center.BestSellers_TopProducts_Inventory_124859654`
    WHERE rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}`),':FR:%'))
    AND product_id LIKE '%:FR:%'
  )

  SELECT
    category_name,
    LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
    COUNT(1) as total_products,
    COUNT(IF(pi.product_id IS NULL, 1, NULL)) AS missing_products,
    COUNT(IF(pi.product_id IS NOT NULL, 1, NULL)) AS available_products,
  FROM best_sellers as bs
  LEFT JOIN inventory as pi
  ON (bs.rank_id = pi.rank_id)
  GROUP BY 1,2
)