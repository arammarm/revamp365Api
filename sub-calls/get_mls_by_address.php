<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$maxLimit = 10;
APIHelper::Authentication();

$test = APIHelper::GetParam('test');
$text = APIHelper::GetParam('address', true);
$tin = APIHelper::GetParam('tin', true);

//if (($text != null && $text != '') || ($tin != null && $tin != '')) {
//    $query = pg_query_params($db, "SELECT id, mls_number, geo_address, tax_id_number, listing_entry_date, status , close_date, close_price, list_price
//                                    FROM properties WHERE LOWER(translate(geo_address,',', '')) LIKE LOWER(translate($1,',', '')) OR TRIM(tax_id_number) = TRIM($2) ORDER BY close_date DESC LIMIT 10", ['%' . $text . '%', $tin]);
//    $result = pg_fetch_all($query);
//    if ($result) {
//         APIHelper::SendResponse($result, 1);
//        exit();
//    }
//}
$zip = null;
// Match zip code from the end of the address (zip codes typically appear at the end)
if (preg_match('/\b\d{5}(?:-\d{4})?\b(?:\s*(?:usa|us))?$/i', trim($text), $matches)) {
    $zip = $matches[0];
} else {
    // Fallback: if not at end, find the last occurrence of a 5-digit number
    if (preg_match_all('/\b\d{5}(?:-\d{4})?\b/', $text, $allMatches)) {
        $zip = end($allMatches[0]);
    }
}

if (($text != null && $text != '')) {
    $params = [$text . '%'];
    $whereClause = $zip ? ' AND zip_code = $2 ' : ''; // Uncomment after other optimizations done.
    if ($whereClause != '') {
        $params[] = trim($zip);
    }
    $query = pg_query_params($db, "SELECT id, mls_number, geo_address, tax_id_number, listing_entry_date, 
    status , close_date, close_price, list_price
                                    FROM properties WHERE clean_address LIKE LOWER(translate($1,',', '')) $whereClause
                                                    ORDER BY close_date DESC LIMIT 15", $params);
    $result = pg_fetch_all($query);
    if ($result) {
        APIHelper::SendResponse($result, 1);
        exit();
    }
}

APIHelper::SendResponse([], 0, 'The request or response is invalid.');