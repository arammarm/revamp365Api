<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$maxLimit = 10;
APIHelper::Authentication();

$paramState = APIHelper::GetParam('state', true);
$paramCounty = APIHelper::GetParam('county', true);
$paramCity = APIHelper::GetParam('city', true);
$paramZipCode = APIHelper::GetParam('zip_code', true);

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

$query = "SELECT status,
       COUNT(CASE
                 WHEN status = 'Closed'
                     AND close_date IS NOT NULL
                     AND close_date::text != ''
                     AND close_date::text != '0000-00-00'
                     AND close_date::date >= NOW() - INTERVAL '7 days'
                     THEN id
                 WHEN status = 'Pending'
                     AND modification_timestamp IS NOT NULL
                     AND modification_timestamp::text != ''
                     AND modification_timestamp::text != '0000-00-00'
                     AND modification_timestamp::date >= NOW() - INTERVAL '7 days'
                     THEN id
                WHEN status NOT IN ('Pending', 'Closed') THEN id
                 ELSE NULL
           END)::int AS filtered_count
FROM properties WHERE true
$sqlParamQuery
GROUP BY status;";

$query = pg_query_params($db, $query, $sqlParams);
$result = pg_fetch_all($query);
if ($result) {
    $totalCount = 0;
    $data = [];
    foreach ($result as $item) {
        $totalCount += $item['filtered_count'];
    }
    foreach ($result as $item) {
        $data[str_replace(' ', '', $item['status'])] = [
            'count' => floatval($item['filtered_count']),
            'percentage' => getStatusPercentage($totalCount, $item['filtered_count']),
        ];
    }
    APIHelper::SendResponse($data, 1);
    exit();
}

APIHelper::SendResponse([], 0, 'The request or response is invalid.');

function getStatusPercentage($totalCount, $value): float
{
    return round(($value / $totalCount) * 100, 2);
}