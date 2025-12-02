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

$templateId = APIHelper::GetParam('template_id', true);
$folderId = APIHelper::GetParam('folder_id', true);
$templateName = APIHelper::GetParam('template_name', true);

$pdfMapData = APIHelper::GetParam('pdf_map_data', true);

$pdfMapDataDecode = json_decode(stripslashes($_REQUEST['pdf_map_data']), true);
//
//print_r($pdfMapDataDecode);
//
//die();

//
//print_r($pdfMapDataDecode); die();

//file_put_contents(__DIR__ . '/_d_.log', json_encode($_REQUEST));
//die();

//print_r($pdfMapDataDecode);
////print_r(stripslashes(preg_replace('/\\\\[ntr]/', '', $_REQUEST['pdf_map_data'])));
//
//die();
//
//$folderId = '2805219';

$errorMessage = 'The request or response is invalid.';
if ($propertyId != null && $templateId != null && $templateName != null && $folderId != null && is_array($pdfMapDataDecode)) {
    $qur = pg_query_params($db, "SELECT *, LPAD(CAST(zip_code AS TEXT), 5, '0') as zip_code FROM properties WHERE id=$1", [$propertyId]);
    $response = pg_fetch_assoc($qur);
    $currentDate = date('Y-m-d');
    if ($response) {
        $pdfArray = [];
        foreach ($pdfMapDataDecode as $pdfMapValue) {
            if (!$pdfMapValue['option_set_custom']) {
                $pdfArray[$pdfMapValue['template_field']] = setValueByType($pdfMapValue['option_set_type'], $response, $pdfMapValue['option_set_field'], $pdfMapValue['template_field_type']);
            } else {
                $pdfArray[$pdfMapValue['template_field']] = PdfFillerAPI::getCustomValues($response, $pdfMapValue['option_set_display']);
                echo PHP_EOL;
            }
        }
        $pdfResponse = (new PdfFillerAPI)->fillTemplate($templateId, $folderId, $templateName, $pdfArray);
        if (!isset($pdfResponse['errors'])) {
             APIHelper::SendResponse($pdfResponse, 1);
            exit();
        } else {
            $errorMessage = @json_encode($pdfResponse['errors']);
        }
    }
}

 APIHelper::SendResponse([], 0, $errorMessage);






