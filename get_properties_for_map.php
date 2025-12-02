<?php
ini_set('memory_limit', '4G');
require_once '../config.php';
require_once 'includes/APIHelper.php';
global $db;
$test = APIHelper::GetParam('test');
$distanceMax = APIHelper::GetParam('distance_max');
$userLat = APIHelper::GetParam('user_lat');
$userLng = APIHelper::GetParam('user_lng');
$maxResultLimit = 5000;
$allWholesale = APIHelper::GetParam('all_wholesale');
//$maxResultLimit = 10;
if ($test == 1) {
    $maxResultLimit = 10;
}

APIHelper::Authentication();

file_put_contents("data_list.log", json_encode($_GET));

$queryParams = [];
$whereClause = ' WHERE 1 = 1 AND is_calculated = true AND bubble_sync = true ';
foreach (APIHelper::$filterPropertiesItems as $filterKey => $filterItem) {
    $whereClause .= APIHelper::GetPropertyFilterQuery($queryParams, $filterKey, $filterItem[0], $filterItem[1]);
}

$orderByQuery = APIHelper:: GetPropertySortBy();

//print_r([$whereClause, $queryParams]);
//die();
$compProp = null;
if (APIHelper::GetParam('comps_sub_prop_id')) {
    $compPropQ = pg_query_params($db, 'SELECT latitude, longitude FROM properties WHERE id = $1', [APIHelper::GetParam('comps_sub_prop_id')]);
    $result = pg_fetch_assoc($compPropQ);
    if ($result) {
        $compProp = $result;
    }
}

$maxLimit = $maxResultLimit;
if ($distanceMax && $userLat && $userLng) {
    $maxLimit = 'ALL';
}

$query = pg_query($db, "SELECT data_type, column_name  FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'properties';");
$result = pg_fetch_all($query);
//$columnKeys = ['real_dom' => ''];
foreach ($result as $queryOption) {
    $columnKeys[$queryOption['column_name']] = $queryOption['data_type'];
}
$orderByQuery = 'id';

if ($allWholesale == '1') {
    $whereClause = str_replace('WHERE ', '', $whereClause);
    $whereClause = " WHERE ( wholesale = 'Wholesale' AND is_calculated = true AND bubble_sync = true AND status = 'Active' ) OR ($whereClause) ";
}

$query = pg_query_params($db, "SELECT id,latitude,longitude, geo_address, list_price, status, full_location, wholesale
FROM properties $whereClause ORDER BY $orderByQuery LIMIT $maxLimit", $queryParams);
$result = pg_fetch_all($query);

if ($distanceMax && $userLat && $userLng) {
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
    }
    $result = $apiData;
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
        $priceInFormat = number_format($item['list_price'], 2, '.', ',');
        $images = explode(',', $item['full_location']);
        $mainImage = ($item['full_location'] != '') ? $images[0] : '//s3.amazonaws.com/appforest_uf/f1650658313166x774273891331084700/coming%20soon4.JPG';
        $address = str_replace(', USA', '', $item['geo_address']);
        $indItem['geo_address'] = $address;
        $indItem['main_image'] = $mainImage;
        $indItem['popup_html'] = '';
//        $indItem['popup_html'] = "<style>.leaflet-popup-content{margin: 0;width: 300px !important;}
//.leaflet-container a.leaflet-popup-close-button{font: 24px / 12px Tahoma, Verdana, sans-serif;width: 24px;height: 24px;background: rgb(255,255,255);border-radius: 15px;padding: 4px 0 0 0;margin: 4px}
//</style>
//<div style='height: 280px'><img style='object-fit: cover;height: 200px;width: 300px;border-top-left-radius: 11px;border-top-right-radius: 11px;' src='$mainImage' alt='property-image_${item['id']}'/><div style='padding:10px'>$address <br><span style='font-weight: bold; font-size: 1.2em'>$$priceInFormat</span><br>${item['status']}&nbsp;&nbsp;<a target='_blank' href='?recordid=${item['id']}&sr=1&ov=1'>Click to view</a></div></div>";
//
        unset($indItem['full_location']);
        unset($indItem['wholesale']);
        $apiData[] = $indItem;
    }
    $nearData = [];
    if (count($nearData) > 1) {
        $apiData = $nearData;
    }
    if (count($apiData) > $maxResultLimit) {
        $apiData = array_slice($apiData, 0, $maxResultLimit);
    }
     APIHelper::SendResponse($apiData, 1);
    exit();
}

 APIHelper::SendResponse([], 0, 'The request or response is invalid.');

