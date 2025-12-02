<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$maxLimit = 10;
APIHelper::Authentication();

$paramLimit = APIHelper::GetParam('_limit', true);
$paramRange = APIHelper::GetParam('range', true);
$paramState = APIHelper::GetParam('state', true);
$paramCounty = APIHelper::GetParam('county', true);
$paramCity = APIHelper::GetParam('city', true);
$paramZipCode = APIHelper::GetParam('zip_code', true);

$rangeStart = getPreviousDaysRange($paramRange)['start'];
$rangeEnd = getPreviousDaysRange($paramRange)['end'];

if($paramLimit == null OR $paramLimit > 200 OR $paramLimit == 0 ) {
    $paramLimit = 200;
}


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

$query = "SELECT id,status,wholesale,list_price, ('' || COALESCE(list_agent_first_name,'') || ' ' || COALESCE(list_agent_last_name,'') || ' at ' || COALESCE(list_office_name,'')) as office_info,
       COALESCE(full_street_address,'') || ', ' || COALESCE(city_name,'') || ' ' || COALESCE(state_or_province,'') || ' ' ||  LPAD(zip_code::text, 5, '0')  as address, bedrooms_count, bathrooms_total_count, total_finished_sqft, structure_type,
       medianrent, est_avm, est_arv, est_cashflow, delta_psf, est_profit, latitude, longitude,  SPLIT_PART(full_location, ',', 1) AS image,
       EXTRACT(DAY FROM (NOW() - listing_entry_date)) as real_dom, 
       close_date, buyer_financing, dom, mls_number,low_comps_ppsf,high_comps_ppsf, listing_entry_date, price_per_sqft, close_price, price_per_sqft_closed,list_agent_email,
       seller_avm, seller_arv, seller_est_flip_rehab, seller_est_flip_profit, seller_est_rental_rehab, seller_est_cashflow, seller_avg_rent,accuracy_score_value, accuracy_score_rent,short_list_price
FROM properties
WHERE status = 'Closed'
  AND close_date::text != ''
  AND close_date::text != '0000-00-00'
  AND close_date::date > CURRENT_DATE - '30 days'::interval
  AND avg_p_sqft - price_per_sqft_closed > 50" . $sqlParamQuery . " LIMIT " . $paramLimit;

$query = pg_query_params($db, $query, $sqlParams);
$resultAll = pg_fetch_all($query);

if($resultAll) {
     APIHelper::SendResponse($resultAll, 1);
    exit();
}


 APIHelper::SendResponse([], 0, 'The request or response is invalid.');
exit();