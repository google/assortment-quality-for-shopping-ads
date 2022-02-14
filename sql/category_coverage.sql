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

WITH best_sellers AS (
    SELECT DISTINCT
        rank_timestamp,
        rank,
        rank_id,
        previous_rank,
        ranking_country,
        ranking_category,
        brand,
        google_brand_id,
        google_product_category,
        (SELECT name FROM tp.ranking_category_path WHERE locale = '{language}') as category_name,
        (SELECT name FROM tp.product_title WHERE locale = '{language}')  as product_name,
        price_range.min as price_range_min,
        price_range.max as price_range_max,
        price_range.currency as price_range_currency,
        relative_demand.bucket as relative_demand_bucket,
        _PARTITIONDATE as PARTITIONDATE
    FROM `{projectId}.{datasetId}.BestSellers_TopProducts_{gmcId}`  as tp
    WHERE
        DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{projectId}.{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND ranking_country IN ('{country}')
        AND rank_id LIKE '%:{country}:%'
),
inventory AS (
    SELECT DISTINCT
        rank_id,
        product_id,
        merchant_id,
        aggregator_id
    FROM `{projectId}.{datasetId}.BestSellers_TopProducts_Inventory_{gmcId}`
    WHERE
        DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{projectId}.{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),':{country}:%'))
        AND product_id LIKE '%:{country}:%'
),
best_sellers_with_inventory AS (
    SELECT
        category_name,
        brand,
        LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
        relative_demand_bucket,
        bs.rank_id as rank_id,
        ARRAY_AGG(DISTINCT(product_id) IGNORE NULLS) AS product_id,
    FROM best_sellers as bs
    LEFT JOIN inventory as pi
        ON (bs.rank_id = pi.rank_id)
    GROUP BY 1,2,3,4
)

SELECT
    category_name,
    brand,
    relative_demand_bucket,
    LENGTH(category_name) - LENGTH(REPLACE(category_name, '>', '')) + 1 as category_level,
    COUNT(DISTINCT rank_id) as total_products,
    COUNTIF(product_id IS NULL OR ARRAY_LENGTH(product_id) = 0) AS missing_products,
    COUNTIF(ARRAY_LENGTH(product_id)>0) AS available_products,
FROM best_sellers_with_inventory
GROUP BY 1,2,3

