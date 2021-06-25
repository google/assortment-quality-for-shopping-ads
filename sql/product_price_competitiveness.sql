/*
* Copyright 2021 Google LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

CREATE OR REPLACE VIEW {datasetId}.product_price_competitiveness AS (
  WITH products_from_feed AS (
    SELECT 
    product_id, price.value, price.currency, gtin
    FROM `{datasetId}.Products_{gmcId}`
    WHERE 
      _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM `{datasetId}.Products_{gmcId}`) 
      AND product_data_timestamp = (SELECT MAX(product_data_timestamp) FROM `{datasetId}.Products_{gmcId}`)
  )
  ,
  price_benchmark AS (
    SELECT 
      product_id,
      country_of_sale,
      price_benchmark_value,
      price_benchmark_currency,
      price_benchmark_timestamp
    FROM `{datasetId}.Products_PriceBenchmarks_{gmcId}`
    WHERE 
    _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM `{datasetId}.Products_PriceBenchmarks_{gmcId}`) 
    AND price_benchmark_timestamp = (SELECT MAX(price_benchmark_timestamp) FROM `{datasetId}.Products_PriceBenchmarks_{gmcId}`)
    AND country_of_sale  = 'FR'
  ),
  best_sellers AS (
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
      DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
      AND rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      AND ranking_country IN ('FR')
      AND rcp.locale IN ('fr-FR')
      AND pt.locale IN ('fr-FR', null, '')
      AND rank_id LIKE '%:FR:%'
  ),
  inventory AS (
    SELECT DISTINCT rank_id, product_id, merchant_id, aggregator_id
    FROM `{datasetId}.BestSellers_TopProducts_Inventory_{gmcId}`
    WHERE 
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)),':FR:%'))
    AND product_id LIKE '%:FR:%'
  )

  SELECT DISTINCT 
      rank,
      ranking_country,
      brand,
      category_name,
      product_name,
      price_range_min,
      price_range_max,
      price_range_currency,
      i.product_id,
      gtin,
      value AS product_price_in_feed,
      currency AS product_currency_in_feed,
      price_benchmark_value,
      price_benchmark_currency,
      LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
      CASE WHEN
       i.product_id IS NULL THEN 'MISSING'
       ELSE 'AVAILABLE'
     END AS status
  FROM best_sellers AS bs
  LEFT JOIN inventory AS i
  ON bs.rank_id = i.rank_id
  LEFT JOIN products_from_feed AS pff
  ON i.product_id = pff.product_id
  LEFT JOIN price_benchmark AS pb
  ON i.product_id = pb.product_id
  ORDER BY rank ASC
)