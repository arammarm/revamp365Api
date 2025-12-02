<?php
ini_set('memory_limit', '4G');
require_once '../config.php';
require_once 'includes/APIHelper.php';
global $db;
APIHelper::Authentication();

APIHelper::RemoveParamIfZero();
APIHelper::setAccuracyParams();

$test = APIHelper::GetParam('test');
$distanceMax = APIHelper::GetParam('distance_max');
$userLat = APIHelper::GetParam('user_lat');
$userLng = APIHelper::GetParam('user_lng');

$_limit = APIHelper::GetParam('limit') == null ? APIHelper::GetParam('_limit') : APIHelper::GetParam('limit');

$maxResultLimit = 200;
if ($_limit != null && $_limit < 200) {
    $maxResultLimit = $_limit;
}


file_put_contents("data_list.log", json_encode($_GET));

$queryParams = [];
//$whereClause = ' WHERE 1 = 1 AND is_calculated = true AND bubble_sync = true ';
$whereClause = APIHelper::initialPropertyWhereClause();

foreach (APIHelper::$filterPropertiesItems as $filterKey => $filterItem) {
    $whereClause .= APIHelper::GetPropertyFilterQuery($queryParams, $filterKey, $filterItem[0], $filterItem[1]);
}

$orderByQuery = APIHelper:: GetPropertySortBy();

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
$columnKeys = ['real_dom' => ''];
foreach ($result as $queryOption) {
    $columnKeys[$queryOption['column_name']] = $queryOption['data_type'];
}
$orderByQueryFull = '';
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

$query = pg_query_params($db, "SELECT *,LPAD(zip_code::text, 5, '0') as zip_code, EXTRACT(DAY FROM (NOW() - listing_entry_date)) as real_dom FROM properties $whereClause $orderByQueryFull LIMIT $maxLimit", $queryParams);
$result = pg_fetch_all($query);
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
            if ($distanceMax) {
                if ($indItem['mile_range_from_subject'] > $distanceMax) {
                    continue;
                }
            }
        }
        $apiData[] = $indItem;
    }
    $nearData = [];
    if ($distanceMax && $userLat && $userLng) {
        foreach ($apiData as $apiDatum) {
            $distance = round(GetGeoDistance($userLat, $userLng, $apiDatum['latitude'], $apiDatum['longitude']), 2);
            if ($distance <= $distanceMax) {
                $apiDatum['distance_from_you'] = $distance;
                $nearData[] = $apiDatum;
            }
        }
    }
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
