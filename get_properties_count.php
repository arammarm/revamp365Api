<?php
require_once '../config.php';
require_once 'includes/APIHelper.php';
global $db;
ob_clean();
APIHelper::Authentication();
APIHelper::RemoveParamIfZero();
APIHelper::setAccuracyParams();

$test = APIHelper::GetParam('test');
$distanceMax = APIHelper::GetParam('distance_max');
$userLat = APIHelper::GetParam('user_lat');
$userLng = APIHelper::GetParam('user_lng');
$allWholesale = APIHelper::GetParam('all_wholesale');
//file_put_contents("data.log", json_encode($_GET));
//
//die();

$_limit = APIHelper::GetParam('_limit');

$queryParams = [];
$whereClause = APIHelper::initialPropertyWhereClause();

foreach (APIHelper::$filterPropertiesItems as $filterKey => $filterItem) {
    $whereClause .= APIHelper::GetPropertyFilterQuery($queryParams, $filterKey, $filterItem[0], $filterItem[1]);
}

//if ($test == '1') {
if ($allWholesale == '1') {
    $whereClause = str_replace('WHERE ', '', $whereClause);
    $whereClause = " WHERE ( wholesale = 'Wholesale' AND is_calculated = true AND bubble_sync = true AND status = 'Active' ) OR ($whereClause) ";
}
//}

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


$responseData = null;
if ($distanceMax && $userLat && $userLng) {
    $query = pg_query_params($db, "SELECT id, latitude, longitude, wholesale FROM properties $whereClause", $queryParams);
    $result = pg_fetch_all($query);
    $nearData = [];
    $nearWholesaleData = [];
    if ($result) {
        foreach ($result as $item) {
//            $userLat = '39.9845';
//            $userLng = '-75.586';
            $distance = round(GetGeoDistance($userLat, $userLng, $item['latitude'], $item['longitude']), 2);
            if ($distance <= $distanceMax) {
                $item['distance_from_you'] = $distance;
                $nearData[] = $item;
            } elseif ($allWholesale == '1' && $item['wholesale'] == 'Wholesale') {
                $nearWholesaleData[] = $item;
            }
        }

        if (count($nearData) > 1) {
            if (count($nearWholesaleData) > 0) {
                $nearData = [...$nearWholesaleData, ...$nearData];
            }
            APIHelper::SendResponse(['count' => count($nearData), 'user_distance' => 1], 1);
            exit();
        }
    }
}
if ($distanceMax && $compProp) {
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

    $query = pg_query_params($db, "SELECT id, latitude, longitude, wholesale FROM properties $whereClause", $queryParams);
    $result = pg_fetch_all($query);
    $apiData = [];
    foreach ($result as $item) {
        $indItem['mile_range_from_subject'] = round(GetGeoDistance($compProp['latitude'], $compProp['longitude'], $item['latitude'], $item['longitude']), 2);
        $apiData[] = $indItem;
    }
    if (count($apiData) > 1) {
        APIHelper::SendResponse(['count' => count($apiData), 'user_distance' => 0], 1);
        exit();
    }
}

$query = pg_query_params($db, "SELECT COUNT(id) FROM properties $whereClause LIMIT 1", $queryParams);
if ($row = pg_fetch_assoc($query)) {
    if (isset($row['count'])) {
        $responseData = $row['count'];
    }
    APIHelper::SendResponse(['count' => @intval($responseData), 'user_distance' => 0], 1);
    exit();
}

APIHelper::SendResponse(['count' => $responseData], 0, 'The request or response is invalid.');

exit();

