<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
APIHelper::Authentication();

$test = APIHelper::GetParam('test');
$distanceMax = APIHelper::GetParam('distance_max');
file_put_contents("../data_list_average.log", json_encode($_GET));

$queryParams = [];
$whereClause = ' WHERE 1 = 1 AND is_calculated = true ';
foreach (APIHelper::$filterPropertiesItems as $filterKey => $filterItem) {
    $whereClause .= APIHelper::GetPropertyFilterQuery($queryParams, $filterKey, $filterItem[0], $filterItem[1]);
}

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
//$maxDistance = 0;
//if (APIHelper::GetParam('distance_max')) {
//    $maxDistance = 0;
//}

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
}

$query = pg_query_params($db, "SELECT latitude, longitude, close_price, price_per_sqft, price_per_sqft_closed, total_finished_sqft, dom, bedrooms_count, bathrooms_total_count, lot_sqft FROM properties $whereClause", $queryParams);
$result = pg_fetch_all($query);

if ($result) {
    $avgSales = 0;
    $avgPPSF = 0;
    $avgClosedPPSF = 0;
    $avgSqf = 0;
    $avgDom = 0;
    $avgDist = 0;
    $beds = 0;
    $bath = 0;
    $lots = 0;
    $totalRecords = count($result);
    foreach ($result as $item) {
        $avgSales += floatval($item['close_price']);
        $avgPPSF += floatval($item['price_per_sqft']);
        $avgClosedPPSF += floatval($item['price_per_sqft_closed']);
        $avgSqf += floatval($item['total_finished_sqft']);
        $avgDom += intval($item['dom']);
        $beds += intval($item['bedrooms_count']);
        $bath += intval($item['bathrooms_total_count']);
        $lots += intval($item['lot_sqft']);
        if ($compProp) {
            $thisDis = GetGeoDistance($compProp['latitude'], $compProp['longitude'], $item['latitude'], $item['longitude']);
            $avgDist += $thisDis;
        }
    }

    $apiData['avg_sales'] = round($avgSales / $totalRecords, 2);
    $apiData['avg_ppsf'] = round($avgPPSF / $totalRecords, 2);
    $apiData['avg_closed_ppsf'] = round($avgClosedPPSF / $totalRecords, 2);
    $apiData['avg_sqft'] = round($avgSqf / $totalRecords);
    $apiData['avg_dom'] = round($avgDom / $totalRecords);
    $apiData['avg_dist'] = round($avgDist / $totalRecords, 2);
    $apiData['avg_beds'] = round($beds / $totalRecords);
    $apiData['avg_baths'] = round($bath / $totalRecords);
    $apiData['avg_lot_size'] = round($lots / $totalRecords);
    $apiData['count'] = count($result);

    file_put_contents("../data_list_average.log", PHP_EOL . PHP_EOL . json_encode($apiData), FILE_APPEND);

    APIHelper::SendResponse($apiData, 1);
    exit();
}
APIHelper::SendResponse([], 0, 'The request or response is invalid.');
