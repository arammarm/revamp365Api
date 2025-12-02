<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$maxLimit = 10;
APIHelper::Authentication();

$paramRange = APIHelper::GetParam('range', true);
$paramState = APIHelper::GetParam('state', true);
$paramCounty = APIHelper::GetParam('county', true);
$paramCity = APIHelper::GetParam('city', true);
$paramZipCode = APIHelper::GetParam('zip_code', true);

$rangeStart = getPreviousDaysRange($paramRange)['start'];
$rangeEnd = getPreviousDaysRange($paramRange)['end'];

$sqlParams = [];
$sqlParamQuery = '';
if ($paramCounty) {
    $sqlParams[] = $paramCounty;
    $sqlParamQuery .= ' AND county = $' . count($sqlParams);
}
if ($paramCity) {
    $sqlParams[] = $paramCity;
    $sqlParamQuery .= ' AND city_name = $' . count($sqlParams);
}
if ($paramState) {
    $sqlParams[] = $paramState;
    $sqlParamQuery .= ' AND state_or_province = $' . count($sqlParams);
}
if ($paramZipCode) {
    $sqlParams[] = $paramZipCode;
    $sqlParamQuery .= ' AND zip_code = $' . count($sqlParams);
}

$masterArray = [];

// -- Sold Under Market Value.
$query = "SELECT count(id)
FROM properties
WHERE status = 'Closed'
  AND close_date::text != ''
  AND close_date::text != '0000-00-00'
  AND close_date::date > CURRENT_DATE - '30 days'::interval
  AND avg_p_sqft - price_per_sqft_closed > 50" . $sqlParamQuery;

$query = pg_query_params($db, $query, $sqlParams);
$ResultSoldUnderMarketValue = pg_fetch_assoc($query);
$masterArray['sold_under_market_value'] = setDefaultValues();
if ($ResultSoldUnderMarketValue) {
    $masterArray['sold_under_market_value']['current'] = intval($ResultSoldUnderMarketValue['count']);
}

$_param1 = count($sqlParams) + 1;
$_param2 = count($sqlParams) + 2;
$_param3 = count($sqlParams) + 3;
$_param4 = count($sqlParams) + 4;

$query = "SELECT count(id)
FROM properties
WHERE status = 'Closed'
  AND close_date::text != ''
  AND close_date::text != '0000-00-00'
  AND close_date::date <= CURRENT_DATE - $$_param1::interval
  AND close_date::date > CURRENT_DATE - $$_param2::interval
  AND avg_p_sqft - price_per_sqft_closed > 50" . $sqlParamQuery;
$query = pg_query_params($db, $query, array_merge($sqlParams, [$rangeStart, $rangeEnd]));
$ResultSoldUnderMarketValuePrevious = pg_fetch_assoc($query);
if ($ResultSoldUnderMarketValuePrevious) {
    $masterArray['sold_under_market_value']['previous'] = intval($ResultSoldUnderMarketValuePrevious['count']);
}
$masterArray['sold_under_market_value']['change'] = calculatePercentageChange($masterArray['sold_under_market_value']['previous'], $masterArray['sold_under_market_value']['current']);


// -- Average Data and Closed Counts
$query = "SELECT AVG(close_price) AS average_close_price, AVG(price_per_sqft_closed) AS average_ppsfc, AVG(dom) as average_dom,  COUNT(id) as closed_count
FROM properties
WHERE status = 'Closed'
  AND close_date::text != ''
  AND close_date::text != '0000-00-00'
  AND close_date::date > CURRENT_DATE - '30 days'::interval
      " . $sqlParamQuery;

$query = pg_query_params($db, $query, $sqlParams);
$ResultAverageSalePrice = pg_fetch_assoc($query);

$masterArray['average_sale_price'] = setDefaultValues();
$masterArray['average_ppsfc'] = setDefaultValues();
$masterArray['average_dom'] = setDefaultValues();
$masterArray['closed_count'] = setDefaultValues();
if ($ResultAverageSalePrice) {
    $masterArray['average_sale_price']['current'] = floatval($ResultAverageSalePrice['average_close_price']);
    $masterArray['average_ppsfc']['current'] = floatval($ResultAverageSalePrice['average_ppsfc']);
    $masterArray['average_dom']['current'] = intval($ResultAverageSalePrice['average_dom']);
    $masterArray['closed_count']['current'] = intval($ResultAverageSalePrice['closed_count']);
}

$query = "SELECT  AVG(close_price) AS average_close_price, AVG(price_per_sqft_closed) AS average_ppsfc, AVG(dom) as average_dom,  COUNT(id) as closed_count
FROM properties
WHERE status = 'Closed'
  AND close_date::text != ''
  AND close_date::text != '0000-00-00'
  AND close_date::date <= CURRENT_DATE - $$_param1::interval
  AND close_date::date > CURRENT_DATE - $$_param2::interval
      " . $sqlParamQuery;

$query = pg_query_params($db, $query, array_merge($sqlParams, [$rangeStart, $rangeEnd]));
$ResultAverageSalePricePrevious = pg_fetch_assoc($query);
if ($ResultAverageSalePricePrevious) {
    $masterArray['average_sale_price']['previous'] = floatval($ResultAverageSalePricePrevious['average_close_price']);
    $masterArray['average_ppsfc']['previous'] = floatval($ResultAverageSalePricePrevious['average_ppsfc']);
    $masterArray['average_dom']['previous'] = intval($ResultAverageSalePricePrevious['average_dom']);
    $masterArray['closed_count']['previous'] = intval($ResultAverageSalePricePrevious['closed_count']);
}
$masterArray['average_sale_price']['change'] = calculatePercentageChange($masterArray['average_sale_price']['previous'], $masterArray['average_sale_price']['current']);
$masterArray['average_ppsfc']['change'] = calculatePercentageChange($masterArray['average_ppsfc']['previous'], $masterArray['average_ppsfc']['current']);
$masterArray['average_dom']['change'] = calculatePercentageChange($masterArray['average_dom']['previous'], $masterArray['average_dom']['current']);
$masterArray['closed_count']['change'] = calculatePercentageChange($masterArray['closed_count']['previous'], $masterArray['closed_count']['current']);


// -- Property Counts - Low Row
$query = "SELECT COUNT(CASE WHEN status = 'Pending' THEN id END) as pending_count,
       COUNT(CASE
                 WHEN
                             listing_entry_date::text != ''
                         AND listing_entry_date::text != '0000-00-00'
                         AND listing_entry_date::date > CURRENT_DATE - '30 days'::interval THEN id END) as new_count,
       COUNT(CASE
                 WHEN
                             listing_entry_date::text != ''
                         AND listing_entry_date::text != '0000-00-00'
                         AND listing_entry_date::date <= CURRENT_DATE - $$_param1::interval
  AND listing_entry_date::date > CURRENT_DATE - $$_param2::interval
                     THEN id END) as new_count_previous
FROM properties WHERE true
" . $sqlParamQuery;

$query = pg_query_params($db, $query, array_merge($sqlParams, [$rangeStart, $rangeEnd]));
$ResultListingCount = pg_fetch_assoc($query);

$masterArray['pending_count'] = setDefaultValues();
$masterArray['new_listings'] = setDefaultValues();
if ($ResultListingCount) {
    $masterArray['pending_count']['current'] = intval($ResultListingCount['pending_count']);
    $masterArray['new_listings']['current'] = intval($ResultListingCount['new_count']);
    $masterArray['new_listings']['previous'] = intval($ResultListingCount['new_count_previous']);
}
$masterArray['new_listings']['change'] = calculatePercentageChange($masterArray['new_listings']['previous'], $masterArray['new_listings']['current']);

// -- Flip property counts
$query = "WITH PropertySales AS (SELECT tax_id_number,
                              full_street_address,
                              city_name,
                              state_or_province,
                              zip_code,
                              county,
                              mls_number,
                              status,
                              CAST(close_date AS DATE)     AS close_date,
                              CAST(close_price AS DECIMAL) AS sold_price,
                              dom
                       FROM properties
                       WHERE status = 'Closed'
                         AND close_date IS NOT NULL
                         AND close_date::text != ''
                         AND close_date::text != '0000-00-00'
                         AND close_date::date > CURRENT_DATE - '12 months'::interval $sqlParamQuery),
     RankedSales AS (SELECT tax_id_number,
                            full_street_address,
                            city_name,
                            state_or_province,
                            zip_code,
                            county,
                            mls_number,
                            close_date,
                            sold_price,
                            dom,
                            ROW_NUMBER() OVER (PARTITION BY full_street_address ORDER BY close_date DESC) AS rank
                     FROM PropertySales),
     FlippedProperties AS (SELECT current.tax_id_number,
                                  current.full_street_address,
                                  current.city_name,
                                  current.state_or_province,
                                  current.zip_code,
                                  current.mls_number                                                     AS latest_mls_number,
                                  current.close_date                                                     AS latest_close_date,
                                  current.sold_price                                                     AS latest_sold_price,
                                  current.dom                                                            AS flip_days_on_market,
                                  previous.mls_number                                                    AS previous_mls_number,
                                  previous.close_date                                                    AS previous_close_date,
                                  previous.sold_price                                                    AS previous_sold_price,
                                  current.sold_price - previous.sold_price                               AS flip_profit,
                                  (CAST(current.close_date AS DATE) - CAST(previous.close_date AS DATE)) AS flip_hold_days
                           FROM RankedSales current
                                    INNER JOIN
                                RankedSales previous
                                ON
                                            current.full_street_address = previous.full_street_address
                                        AND current.rank = 1
                                        AND previous.rank = 2
                                        AND CAST(current.close_date AS DATE) > CAST(previous.close_date AS DATE)
                                        AND EXTRACT(YEAR FROM AGE(CAST(current.close_date AS DATE),
                                                                  CAST(previous.close_date AS DATE))) * 12 +
                                            EXTRACT(MONTH FROM AGE(CAST(current.close_date AS DATE),
                                                                   CAST(previous.close_date AS DATE))) <= 24
                                        AND current.sold_price > previous.sold_price)
SELECT COUNT(*) AS flipped_properties_count
FROM FlippedProperties;";

$query = pg_query_params($db, $query, $sqlParams);
$ResultFlipped = pg_fetch_assoc($query);

$masterArray['flip_count'] = setDefaultValues();
if ($ResultFlipped) {
    $masterArray['flip_count']['current'] = intval($ResultFlipped['flipped_properties_count']);
}

$query = "WITH PropertySales AS (SELECT tax_id_number,
                              full_street_address,
                              city_name,
                              state_or_province,
                              zip_code,
                              county,
                              mls_number,
                              status,
                              CAST(close_date AS DATE)     AS close_date,
                              CAST(close_price AS DECIMAL) AS sold_price,
                              dom
                       FROM properties
                       WHERE status = 'Closed'
                         AND close_date::text != ''
                         AND close_date::text != '0000-00-00'
                         AND close_date::date <= CURRENT_DATE - '12 months'::interval
                         AND close_date::date > CURRENT_DATE - '24 months'::interval 
                         $sqlParamQuery),
     RankedSales AS (SELECT tax_id_number,
                            full_street_address,
                            city_name,
                            state_or_province,
                            zip_code,
                            county,
                            mls_number,
                            close_date,
                            sold_price,
                            dom,
                            ROW_NUMBER() OVER (PARTITION BY full_street_address ORDER BY close_date DESC) AS rank
                     FROM PropertySales),
     FlippedProperties AS (SELECT current.tax_id_number,
                                  current.full_street_address,
                                  current.city_name,
                                  current.state_or_province,
                                  current.zip_code,
                                  current.mls_number                                                     AS latest_mls_number,
                                  current.close_date                                                     AS latest_close_date,
                                  current.sold_price                                                     AS latest_sold_price,
                                  current.dom                                                            AS flip_days_on_market,
                                  previous.mls_number                                                    AS previous_mls_number,
                                  previous.close_date                                                    AS previous_close_date,
                                  previous.sold_price                                                    AS previous_sold_price,
                                  current.sold_price - previous.sold_price                               AS flip_profit,
                                  (CAST(current.close_date AS DATE) - CAST(previous.close_date AS DATE)) AS flip_hold_days
                           FROM RankedSales current
                                    INNER JOIN
                                RankedSales previous
                                ON
                                            current.full_street_address = previous.full_street_address
                                        AND current.rank = 1
                                        AND previous.rank = 2
                                        AND CAST(current.close_date AS DATE) > CAST(previous.close_date AS DATE)
                                        AND EXTRACT(YEAR FROM AGE(CAST(current.close_date AS DATE),
                                                                  CAST(previous.close_date AS DATE))) * 12 +
                                            EXTRACT(MONTH FROM AGE(CAST(current.close_date AS DATE),
                                                                   CAST(previous.close_date AS DATE))) <= 24
                                        AND current.sold_price > previous.sold_price)
SELECT COUNT(*) AS flipped_properties_count
FROM FlippedProperties;";

$query = pg_query_params($db, $query, array_merge($sqlParams));
$ResultFlippedPrevious = pg_fetch_assoc($query);


if ($ResultFlippedPrevious) {
    $masterArray['flip_count']['previous'] = intval($ResultFlippedPrevious['flipped_properties_count']);
}
$masterArray['flip_count']['change'] = calculatePercentageChange($masterArray['flip_count']['previous'], $masterArray['flip_count']['current']);

 APIHelper::SendResponse($masterArray, 1);
exit();


function getStatusPercentage($totalCount, $value): float
{
    return round(($value / $totalCount) * 100, 2);
}

function setDefaultValues(): array
{
    return [
        'current' => 0,
        'previous' => 0,
        'change' => 0
    ];
}
