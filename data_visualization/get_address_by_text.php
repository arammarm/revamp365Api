<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$maxLimit = 10;
APIHelper::Authentication();

$test = APIHelper::GetParam('test');
$text = APIHelper::GetParam('text', true);
if ($text != null && $text != '') {
    $query = pg_query_params($db, 'SELECT id, REPLACE(geo_address, \', USA\', \'\') as geo_address, status FROM properties WHERE LOWER(geo_address) LIKE LOWER($1) LIMIT $2', ['%' . $text . '%', $maxLimit]);
    $result = pg_fetch_all($query);
    if ($result) {
         APIHelper::SendResponse($result, 1);
        exit();
    }
}

 APIHelper::SendResponse([], 0, 'The request or response is invalid.');