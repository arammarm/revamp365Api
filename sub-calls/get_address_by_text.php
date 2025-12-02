<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$maxLimit = 10;
APIHelper::Authentication();

$test = APIHelper::GetParam('test');
$text = APIHelper::GetParam('text', true);
$avoidClosed = APIHelper::GetParam('exclude_negative_status', true);

$queryParams = ['%' . $text . '%', $maxLimit];
$whereClause = '';

if (!empty($text)) {
    if ($avoidClosed == 1) {
        $statuses = ['Closed', 'Canceled', 'Withdrawn', 'Expired'];
        $placeholders = [];
        foreach ($statuses as $i => $status) {
            $queryParams[] = $status;
            $placeholders[] = '$' . (count($queryParams)); // position-safe
        }
        $whereClause = ' AND status NOT IN (' . implode(', ', $placeholders) . ')';
    }

    $sql = "
        SELECT id, REPLACE(geo_address, ', USA', '') as geo_address, status,
               SPLIT_PART(full_location, ',', 1) AS image
        FROM properties 
        WHERE LOWER(geo_address) LIKE LOWER($1)
        $whereClause
        LIMIT $2
    ";

    $query = pg_query_params($db, $sql, $queryParams);
    $result = pg_fetch_all($query);

    if ($result) {
        APIHelper::SendResponse($result, 1);
        exit;
    }
}

APIHelper::SendResponse([], 0, 'The request or response is invalid.');