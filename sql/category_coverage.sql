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
        DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
        AND ranking_country IN ('FR')
        AND rcp.locale IN ('fr-FR')
        AND pt.locale IN ('fr-FR', null, '')
        AND rank_id LIKE '%:FR:%'
  ),
  inventory AS (
    SELECT DISTINCT rank_id, product_id, merchant_id, aggregator_id
    FROM `merchant_center.BestSellers_TopProducts_Inventory_124859654`
    WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)),':FR:%'))
    AND product_id LIKE '%:FR:%'
  )

   SELECT 
    category_name, 
    brand,
    LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
    COUNT(DISTINCT rank_id) as total_products, 
    COUNT(IF(product_id IS NULL OR ARRAY_LENGTH(product_id)=0, 1, NULL)) AS missing_products,
    COUNT(IF(ARRAY_LENGTH(product_id)>0, 1, NULL)) AS available_products,
  FROM (
    SELECT
      category_name,
      brand,
      LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
      bs.rank_id as rank_id,
      ARRAY_AGG(DISTINCT(product_id)IGNORE NULLS) AS product_id,
    FROM best_sellers as bs
    LEFT JOIN inventory as pi
      ON (bs.rank_id = pi.rank_id)
    GROUP BY 1,2,3,4
    )
  GROUP BY 1,2,3
)