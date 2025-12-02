<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
global $db;
$test = APIHelper::GetParam('test');
$id = APIHelper::GetParam('id');
$type = APIHelper::GetParam('type');

if ($id && $type) {
    if ($type == 'meta_data') {
        $apiData = [];
        $query = pg_query_params($db, "SELECT id, full_street_address, city_name, state_or_province, zip_code, full_location, status, list_price, total_finished_sqft, bedrooms_count, bathrooms_total_count, est_avm, medianrent, wholesale
FROM properties WHERE id = $1", [$id]);
        $rsData = pg_fetch_assoc($query);
        if ($rsData) {
            $addressPrefix = $rsData['wholesale'] == 'Wholesale' ? 'Off-Market | ' : ($rsData['full_street_address'] . ', ');

            $apiData['address'] = $addressPrefix . $rsData['city_name'] . ' ' . $rsData['state_or_province'] . ' ' . $rsData['zip_code'];
            $apiData['full_title'] = $apiData['address'] . ' - ' . $rsData['status'] . ' - $' . number_format($rsData['list_price'], 2, '.', ',');
            $images = explode(',', $rsData['full_location']);
            $apiData['image'] = $images[0];
            $totalSqft = number_format($rsData['total_finished_sqft'], 0, '.', ',');
            $beds = number_format($rsData['bedrooms_count'], 1, '.', ',');
            $baths = number_format($rsData['bathrooms_total_count'], 1, '.', ',');
            $estAvm = number_format($rsData['est_avm'], 2, '.', ',');
            $rent = number_format($rsData['medianrent'], 0, '.', ',');

            $apiData['description'] = "This ${totalSqft}Sqft house has $beds beds, $baths baths and has an AVM of $$estAvm and a rental estimate of $$rent/per month";
             APIHelper::SendResponse($apiData, 1);
            exit();
        }
    }
}
 APIHelper::SendResponse([], 0, 'The request or response is invalid.');
exit();
