<?php
ini_set('memory_limit', '4G');
//require_once '../../config.php';
require_once '../includes/APIHelper.php';

APIHelper::Authentication();
$test = APIHelper::GetParam('test');
$type = APIHelper::GetParam('type');
$text = APIHelper::GetParam('text');

$apiKey = 'AIzaSyBDNTzaUXUOeu3Fgf7w3Evsa2ADjx_Jmow';

if ($type == 'search') {
    $searchQuery = $text;
    // Prepare the request URL
    $url = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json';
    $url .= '?input=' . urlencode($searchQuery);
    $url .= '&inputtype=textquery';
    $url .= '&components=country:US';
    $url .= '&fields=formatted_address,name,geometry';
    $url .= '&key=' . $apiKey;

    $response = file_get_contents($url);
    $data = json_decode($response, true);

    $resArray = [];
    if ($data['status'] == 'OK') {
        $results = $data['candidates'];
        foreach ($results as $key => $result) {
            $resArray[$key]['address'] = $result['formatted_address'];
            $resArray[$key]['lat'] = $result['geometry']['location']['lat'];
            $resArray[$key]['lng'] = $result['geometry']['location']['lng'];
        }
         APIHelper::SendResponse($resArray, 1);
    } else {
         APIHelper::SendResponse([], 0, $data['error_message']);
    }
    exit();
} elseif ($type == 'getgeo') {
    $addressResponse = [];
    $url = 'https://maps.googleapis.com/maps/api/geocode/json';
    $url .= '?address=' . urlencode($text);
    $url .= '&key=' . $apiKey;

    $response = file_get_contents($url);
    $data = json_decode($response, true);
    if ($data['status'] == 'OK') {
        $addressComponents = $data['results'][0]['address_components'] ?? [];
        $geometry = $data['results'][0]['geometry']['location'] ?? [];

        foreach ($addressComponents as $component) {
            if (in_array('street_number', $component['types'])) {
                $addressResponse['street'] = $component['long_name'] . " ";
            } else if (in_array('route', $component['types'])) {
                $addressResponse['street'] .= $component['long_name'];
            } else if (in_array('locality', $component['types'])) {
                $addressResponse['city'] = $component['long_name'];
            } else if (in_array('administrative_area_level_1', $component['types'])) {
                $addressResponse['state'] = $component['short_name'];
            } else if (in_array('administrative_area_level_2', $component['types'])) {
                $addressResponse['county'] = trim(str_replace('County', '', $component['long_name']));
            } else if (in_array('postal_code', $component['types'])) {
                $addressResponse['zip'] = trim(str_replace('County', '', $component['long_name']));
            }
        }
        $addressResponse['lat'] = $geometry['lat'] ?? null;
        $addressResponse['lng'] = $geometry['lng'] ?? null;

         APIHelper::SendResponse($addressResponse, 1);
        exit();
    } else {
         APIHelper::SendResponse([], 0, 'Invalid Data');
        exit();
    }
} elseif ($type == 'getgeobygeocode') {
    $addressResponse = [];
    $url = 'https://maps.googleapis.com/maps/api/geocode/json';
    $url .= '?address=' . urlencode($text);
    $url .= '&key=' . $apiKey;

    $response = file_get_contents($url);

    $data = json_decode($response, true);
    if ($data['status'] == 'OK') {
        $addressComponents = $data['results'][0]['address_components'];
        foreach ($addressComponents as $component) {
            if (in_array('street_number', $component['types'])) {
                $addressResponse['street'] = $component['long_name'] . " ";
            } else if (in_array('route', $component['types'])) {
                $addressResponse['street'] .= $component['long_name'];
            } else if (in_array('locality', $component['types'])) {
                $addressResponse['city'] = $component['long_name'];
            } else if (in_array('administrative_area_level_1', $component['types'])) {
                $addressResponse['state'] = $component['short_name'];
            } else if (in_array('administrative_area_level_2', $component['types'])) {
                $addressResponse['county'] = trim(str_replace('County', '', $component['long_name']));
            } else if (in_array('postal_code', $component['types'])) {
                $addressResponse['zip'] = trim(str_replace('County', '', $component['long_name']));
            }
        }
         APIHelper::SendResponse($addressResponse, 1);
        exit();
    } else {
         APIHelper::SendResponse([], 0, 'Invalid Data');
        exit();
    }
}
 APIHelper::SendResponse([], 0, 'Invalid Data');
exit();