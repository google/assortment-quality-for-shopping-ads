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

WITH top_brands AS (
    SELECT DISTINCT
        rank_timestamp,
        rank_id,
        rank,
        ranking_category,
        (SELECT name FROM tp.ranking_category_path WHERE locale = 'fr-FR') as category_name,
        brand,
    FROM `{projectId}.{datasetId}.BestSellers_TopBrands_{gmcId}` AS tp
    LEFT JOIN tp.ranking_category_path AS rcp
    WHERE
        DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{projectId}.{datasetId}.BestSellers_TopBrands_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND ranking_country = 'FR'
        AND brand IS NOT NULL
),
products_in_brand AS (
    SELECT DISTINCT
        tp.rank_timestamp,
        tb.ranking_category,
        (SELECT name FROM tp.ranking_category_path WHERE locale = 'fr-FR') as ranking_category_name,
        tb.category_name,
        tb.brand,
        tp.rank_id,
        tb.rank
    FROM `{projectId}.{datasetId}.BestSellers_TopProducts_{gmcId}` tp
    LEFT JOIN top_brands tb
        ON tb.brand = tp.brand
        AND tb.ranking_category = tp.ranking_category
        AND tp.rank_timestamp = tb.rank_timestamp
    WHERE
        DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND tp.rank_timestamp = (SELECT MAX(rank_timestamp) FROM `{projectId}.{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND ranking_country = 'FR'
        AND tb.brand IS NOT NULL
),
inventory AS (
    SELECT DISTINCT
        rank_id,
        product_id
    FROM `{projectId}.{datasetId}.BestSellers_TopProducts_Inventory_{gmcId}`
    WHERE
        DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND rank_id LIKE (CONCAT((SELECT MAX(CAST(rank_timestamp AS Date)) FROM `{projectId}.{datasetId}.BestSellers_TopProducts_{gmcId}` WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),':FR:%'))
        AND product_id LIKE '%:FR:%'
)

SELECT
    rank,
    pib.ranking_category,
    pib.category_name,
    (
    LENGTH(pib.ranking_category_name)
    - LENGTH(REPLACE(pib.ranking_category_name,'>',''))
    ) + 1 AS category_level,
    pib.brand,
    COUNT(tpi.rank_id) AS products_in_inventory,
    COUNT(pib.rank_id) as number_of_products,
    COUNT(tpi.rank_id) / COUNT(pib.rank_id) as assortment
FROM products_in_brand AS pib
LEFT JOIN inventory tpi
    ON pib.rank_id = tpi.rank_id
GROUP BY 1,2,3,4,5