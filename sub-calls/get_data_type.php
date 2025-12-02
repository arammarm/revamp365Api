<?php
if ($_REQUEST['type'] == 'msl-offer-pid-op') {
    $pIds = getArray($_REQUEST['property_ids'] ?? '');
    $oPrices = getArray($_REQUEST['offer_prices'] ?? '');
//    $oPricesInText = getArray($_REQUEST['offer_prices_in_text'] ?? '');

    if ($pIds) {
        $data = [];
        foreach ($pIds as $pIndex => $pId) {
            $data[] = ['index' => $pIndex + 1, 'product_id' => trim($pId), 'offer_price' => trim($oPrices[$pIndex] ?? ''), 'offer_price_in_text' => getNumberInText(trim($oPrices[$pIndex] ?? ''))];
        }
        echo _response($data);
        exit();
    }
}

function _response($data)
{
    header('content-type: application/json');
    ob_clean();
    return json_encode($data);
}

function getArray($string, $sep = ',')
{
    if ($string != '') {
        return explode($sep, $string);
    }
}

function getNumberInText($number){
    if($number){
        $locale = 'en';
        $fmt = new NumberFormatter($locale, NumberFormatter::SPELLOUT);
        return ucwords($fmt->format($number));
    }
  return '';
}