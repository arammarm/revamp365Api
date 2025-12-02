<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
require_once '../includes/PdfFillerAPI.php';
global $db;

$maxLimit = 10;
APIHelper::Authentication();
$test = APIHelper::GetParam('test');
$propertyId = APIHelper::GetParam('property_id', true);

$bodyText = APIHelper::GetParam('body_text', true);
$mapMappingData = PdfFillerAPI::getOptionSet();

$errorMessage = 'The request or response is invalid.';
if ($propertyId != null && is_array($mapMappingData)) {
    $qur = pg_query_params($db, "SELECT *, LPAD(CAST(zip_code AS TEXT), 5, '0') as zip_code FROM properties WHERE id=$1", [$propertyId]);
    $response = pg_fetch_assoc($qur);

    $currentDate = date('Y-m-d');
    if ($response) {
        $emailBody = $bodyText;
        $tagsArray = [];
        foreach ($mapMappingData as $mappingValue) {
            $mapVField = $mappingValue['field'];
            $mapVDisplay = $mappingValue['display'];
            $mapVCustom = $mappingValue['custom'];
            $mapVType = $mappingValue['type'];

            if (!$mapVCustom) {
                $tagsArray[$mapVDisplay] = setValueByType($mapVType, $response, $mapVField);
            } else {
                $tagsArray[$mapVDisplay] = PdfFillerAPI::getCustomValues($response, $mapVDisplay);
                echo PHP_EOL;
            }
        }
        if (!empty($tagsArray)) {
            $emailBody = PdfFillerAPI::replaceTags($emailBody, $tagsArray);
             APIHelper::SendResponse($emailBody, 1);
            exit();
        }
    }
}

 APIHelper::SendResponse([], 0, $errorMessage);









