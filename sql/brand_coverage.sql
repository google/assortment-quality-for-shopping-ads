CREATE OR REPLACE VIEW {datasetId}.brand_coverage AS (
  WITH top_brands AS (
    SELECT DISTINCT
      rank_timestamp,
      rank_id,
      rank,
      ranking_category,
      rcp.name AS ranking_category_path_name,
      brand,
    FROM `{projectId}.{datasetId}.BestSellers_TopBrands_{gmcId}` AS tp
    LEFT JOIN tp.ranking_category_path AS rcp
    WHERE
      DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{datasetId}.BestSellers_TopBrands_{gmcId}` WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
      AND ranking_country = 'FR'
      AND rcp.locale IN ('fr-FR', null)
      AND brand IS NOT NULL
  ),
  products_in_brand AS (
    SELECT DISTINCT
      tp.rank_timestamp,
      tp.ranking_category,
      ranking_category_path_name,
      tb.brand,
      tp.rank_id,
      tb.rank
    FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` tp
    LEFT JOIN tp.ranking_category_path rcp
    LEFT JOIN top_brands tb
      ON tb.brand = tp.brand
      AND tb.ranking_category = tp.ranking_category
      AND tp.rank_timestamp = tb.rank_timestamp
    WHERE
      DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND tp.rank_timestamp IN (SELECT MAX(rank_timestamp) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
      AND ranking_country = 'FR'
      AND rcp.locale IN ('fr-FR', null)
      AND tb.brand IS NOT NULL
  ),
  inventory AS (
    SELECT DISTINCT rank_id, product_id
    FROM `{datasetId}.BestSellers_TopProducts_Inventory_{gmcId}`
    WHERE
      DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),':FR:%'))
    AND product_id LIKE '%:FR:%'
  )

  SELECT
      rank,
      pib.ranking_category,
      pib.ranking_category_path_name,
      (
        LENGTH(pib.ranking_category_path_name)
        - LENGTH(REPLACE(pib.ranking_category_path_name,'>',''))
      ) + 1 AS category_level,
      pib.brand,
      COUNT(pib.rank_id) as number_of_products,
      COUNT(tpi.rank_id) / COUNT(pib.rank_id) as assortment
  FROM products_in_brand AS pib
  LEFT JOIN inventory tpi
  ON pib.rank_id = tpi.rank_id
  GROUP BY 1,2,3,4,5
)