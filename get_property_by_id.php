<?php
require_once '../config.php';
require_once 'includes/APIHelper.php';
global $db;

APIHelper::Authentication();
$queryParams = [];
$whereClause = ' WHERE bubble_sync = true ';
if (APIHelper::GetParam('id') != null) {
    $queryParams [] = APIHelper::GetParam('id');
    $whereClause .= " AND id = $" . count($queryParams);

    $query = pg_query($db, "
SELECT data_type, column_name
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'properties';
");
    $result = pg_fetch_all($query);
    $columnKeys = [];
    $columnKeys = ['real_dom'=> ''];
    foreach ($result as $queryOption) {
        $columnKeys[$queryOption['column_name']] = $queryOption['data_type'];
    }
    $query = pg_query_params($db, "SELECT *, EXTRACT(DAY FROM (NOW() - listing_entry_date)) as real_dom FROM properties $whereClause LIMIT 1", $queryParams);
    if ($row = pg_fetch_assoc($query)) {
        $indItem = [];
        foreach ($row as $itemColumnKey => $itemColumnValue) {
            $indItem[$itemColumnKey] = isNumberDataType($columnKeys[$itemColumnKey]) ? (float)$itemColumnValue : $itemColumnValue;
        }

         APIHelper::SendResponse([$indItem], 1);
        exit();
    }
}
 APIHelper::SendResponse([], 0, 'The request or response is invalid.');