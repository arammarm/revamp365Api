<?php
ini_set('memory_limit', '4G');
require_once '../config.php';
require_once 'includes/APIHelperV2.php';
global $db;

APIHelper::Authentication();


APIHelper::RemoveParamIfZero();
APIHelper::setAccuracyParams();

$test = APIHelper::GetParam('test');
$distanceMax = APIHelper::GetParam('distance_max');
$userLat = APIHelper::GetParam('user_lat');
$userLng = APIHelper::GetParam('user_lng');
$allWholesale = APIHelper::GetParam('all_wholesale');
$maxResultLimit = 5000;

$initialTime = microtime(true);

$_limit = APIHelper::GetParam('_limit');
if ($_limit != null && $_limit < 5000) {
    $maxResultLimit = $_limit;
} 

file_put_contents("data_list.log", json_encode($_GET));

$queryParams = [];
$whereClause = APIHelper::initialPropertyWhereClause();

foreach (APIHelper::$filterPropertiesItems as $filterKey => $filterItem) {
    $whereClause .= APIHelper::GetPropertyFilterQuery($queryParams, $filterKey, $filterItem[0], $filterItem[1]);
}

$orderByQuery = APIHelper:: GetPropertySortBy();

// Validate comps lat&lng or proprety ID
$compProp = null;
if (APIHelper::GetParam('comps_sub_prop_id')) {
    $idOrLatLng = APIHelper::GetParam('comps_sub_prop_id');
    $_latLng = explode('|', $idOrLatLng);
    $result = [];
    if (isset($_latLng[1])) {
        $compProp = ['latitude' => $_latLng[0], 'longitude' => $_latLng[1]];
    } else {
        $compPropQ = pg_query_params($db, 'SELECT latitude, longitude FROM properties WHERE id = $1', [$idOrLatLng]);
        $result = pg_fetch_assoc($compPropQ);
        if ($result) {
            $compProp = $result;
        }
    }
}

$maxLimit = $maxResultLimit;
if ($distanceMax && $userLat && $userLng) {
    $maxLimit = 'ALL';
}

$query = pg_query($db, "SELECT data_type, column_name  FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'properties';");
$result = pg_fetch_all($query);

// Add new CUSTOM columns and its type here.[Important]
$columnKeys = ['real_dom' => 'numeric', 'office_info' => 'text', 'image' => 'text', 'address' => 'text', 'county_state' => 'text', 'days_from_now'=> 'numeric'];


foreach ($result as $queryOption) {
    $columnKeys[$queryOption['column_name']] = $queryOption['data_type'];
}
$orderByQueryFull = ' ORDER BY wholesale DESC ';
if ($orderByQuery != ' random() ') {
    $orderByQueryFull = ' ORDER BY ' . $orderByQuery;
}

if ($compProp && $distanceMax) {
    $latKey = count($queryParams) + 1;
    $longKey = count($queryParams) + 2;
    $disKey = count($queryParams) + 3;

    $queryParams[$latKey] = $compProp['latitude'];
    $queryParams[$longKey] = $compProp['longitude'];
    $queryParams[$disKey] = $distanceMax;

    $whereClause .= " AND earth_distance(
              ll_to_earth($$latKey,$$longKey),
              ll_to_earth(latitude, longitude)
          ) <= $$disKey * 1609.34 ";
}

if ($allWholesale == '1') {
    $whereClause = str_replace('WHERE ', '', $whereClause);
    $whereClause = " WHERE ( wholesale = 'Wholesale' AND is_calculated = true AND bubble_sync = true AND status = 'Active' ) OR ($whereClause) ";
}

//print_r([$whereClause, $queryParams]);
//die();

if ($test == 1) {
    $query = pg_query_params($db, "SELECT id,status,wholesale,list_price, ('' || COALESCE(list_agent_first_name,'') || ' ' || COALESCE(list_agent_last_name,'') || ' at ' || COALESCE(list_office_name,'')) as office_info,
       COALESCE(full_street_address,'') || ', ' || COALESCE(city_name,'') || ' ' || COALESCE(state_or_province,'') || ' ' ||  LPAD(zip_code::text, 5, '0')  as address, bedrooms_count, bathrooms_total_count, total_finished_sqft, structure_type,
       medianrent, est_avm, est_arv, est_cashflow, delta_psf, est_profit, latitude, longitude,  SPLIT_PART(full_location, ',', 1) AS image,
       EXTRACT(DAY FROM (NOW() - listing_entry_date)) as real_dom, 
       close_date, buyer_financing, dom, mls_number,low_comps_ppsf,high_comps_ppsf, listing_entry_date, price_per_sqft, close_price, price_per_sqft_closed,list_agent_email,
       seller_avm, seller_arv, seller_est_flip_rehab, seller_est_flip_profit, seller_est_rental_rehab, seller_est_cashflow, seller_avg_rent,accuracy_score_value, accuracy_score_rent,short_list_price,
        concat(county, ', ', state_or_province) as county_state, year_built, lot_sqft, school_district_name
FROM properties $whereClause $orderByQueryFull LIMIT $maxResultLimit", $queryParams);
    $result = pg_fetch_all($query);
} else {
    $query = pg_query_params($db, "SELECT id,status,wholesale,list_price, ('' || COALESCE(list_agent_first_name,'') || ' ' || COALESCE(list_agent_last_name,'') || ' at ' || COALESCE(list_office_name,'')) as office_info,
       COALESCE(full_street_address,'') || ', ' || COALESCE(city_name,'') || ' ' || COALESCE(state_or_province,'') || ' ' ||  LPAD(zip_code::text, 5, '0')  as address, bedrooms_count, bathrooms_total_count, total_finished_sqft, structure_type,
       medianrent, est_avm, est_arv, est_cashflow, delta_psf, est_profit, latitude, longitude,  SPLIT_PART(full_location, ',', 1) AS image,
       EXTRACT(DAY FROM (NOW() - listing_entry_date)) as real_dom, 
       close_date, buyer_financing, dom, mls_number,low_comps_ppsf,high_comps_ppsf, listing_entry_date, price_per_sqft, close_price ,price_per_sqft_closed, list_agent_email,
       seller_avm, seller_arv, seller_est_flip_rehab, seller_est_flip_profit, seller_est_rental_rehab, seller_est_cashflow,seller_avg_rent, accuracy_score_value, accuracy_score_rent,short_list_price,
       concat(county, ', ', state_or_province) as county_state, year_built, lot_sqft, school_district_name,  EXTRACT(DAY FROM NOW() - mls_modification_at) AS days_from_now 
FROM properties $whereClause $orderByQueryFull LIMIT $maxLimit", $queryParams);
    $result = pg_fetch_all($query);
}

if ($result) {
    $apiData = [];
    foreach ($result as $item) {
        $indItem = [];
        foreach ($item as $itemColumnKey => $itemColumnValue) {
            if (isset($columnKeys[$itemColumnKey])) {
                $indItem[$itemColumnKey] = isNumberDataType($columnKeys[$itemColumnKey]) ? (float)$itemColumnValue : $itemColumnValue;
            }
        }
        $indItem['mile_range_from_subject'] = 0;
        if ($compProp) {
            $indItem['mile_range_from_subject'] = round(GetGeoDistance($compProp['latitude'], $compProp['longitude'], $item['latitude'], $item['longitude']), 2);
        }
        $apiData[] = $indItem;
    }

    $nearWholesaleData = [];
    $nearData = [];
    if ($distanceMax && $userLat && $userLng) {
        foreach ($apiData as $apiDatum) {
            $distance = round(GetGeoDistance($userLat, $userLng, $apiDatum['latitude'], $apiDatum['longitude']), 2);
            if ($distance <= $distanceMax) {
                $apiDatum['distance_from_you'] = $distance;
                $nearData[] = $apiDatum;
            } elseif ($allWholesale == '1' && $apiDatum['wholesale'] == 'Wholesale') {
                $nearWholesaleData[] = $apiDatum;
            }
        }
    }
    if (count($nearData) > 1) {
        if (count($nearWholesaleData) > 0) {
            $nearData = [...$nearWholesaleData, ...$nearData];
        }
        $apiData = $nearData;
    }

    if (count($apiData) > $maxResultLimit) {
        $apiData = array_slice($apiData, 0, $maxResultLimit);
    }
    if ($test == 2) {
        print_r($apiData[0]);
        echo microtime(true) - $initialTime . PHP_EOL;
        die();
    }
    APIHelper::SendResponse($apiData, 1);
    exit();
}

APIHelper::SendResponse([], 1, 'The request or response is invalid.');
