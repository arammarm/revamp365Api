<?php
ini_set('memory_limit', '4G');
require_once '../../config.php';
require_once '../includes/APIHelper.php';
$apiKey = 'AIzaSyBDNTzaUXUOeu3Fgf7w3Evsa2ADjx_Jmow';
global $db;
APIHelper::Authentication();

$test = APIHelper::GetParam('test');
$zipCode = APIHelper::GetParam('zip_code');
$propertyType = APIHelper::GetParam('property_type');
$lat = APIHelper::GetParam('latitude');
$lng = APIHelper::GetParam('longitude');
$sqft = APIHelper::GetParam('sqft');
$listPrice = APIHelper::GetParam('list_price');
$address = APIHelper::GetParam('address');

$structureTypes = getStructureTypes($propertyType, $address);

if (!empty($zipCode) && !empty($sqft)) {
    $accuracyScore = 9.9;
    $compsItemQuery = pg_query_params($db, "WITH avg_ppsqft AS (
  SELECT zip_code,
         avg(price_per_sqft_closed) AS avg_ppsqft
  FROM properties
  WHERE status = $1
    AND close_date::date > CURRENT_DATE - INTERVAL '6 months'
    AND total_finished_sqft IS NOT NULL AND total_finished_sqft <> 0
    AND price_per_sqft_closed IS NOT NULL AND price_per_sqft_closed <> 0
  GROUP BY zip_code
)
SELECT p.id,
       p.structure_type, 
       p.latitude,
       p.longitude,
       p.total_finished_sqft
FROM properties p
JOIN avg_ppsqft a ON a.zip_code = p.zip_code
WHERE p.status = $1
  AND p.zip_code = $2
  AND p.close_date::date > CURRENT_DATE - INTERVAL '6 months'
  AND p.total_finished_sqft IS NOT NULL AND p.total_finished_sqft <> 0
  AND p.price_per_sqft_closed IS NOT NULL AND p.price_per_sqft_closed <> 0
  AND p.price_per_sqft_closed <= (3 * a.avg_ppsqft);"
        , ['Closed', $zipCode]);

    $compsResults = pg_fetch_all($compsItemQuery);

    $filteredCompsRecs = [];
    foreach ($compsResults as $compsItem) {
        if (in_array($compsItem['structure_type'], $structureTypes)) {
            $filteredCompsRecs[] = $compsItem;
        }
    }
    $matchedCompsIds = [];
    if (!empty($filteredCompsRecs)) {
        $matchedCompsIds = getMileRangePropertiesIds($filteredCompsRecs, $lat, $lng);
        if (!empty($matchedCompsIds)) {
            $matchedCompsIds = getSqftRangePropertiesIds($filteredCompsRecs, $sqft, $matchedCompsIds);
        } else {
            $matchedCompsIds = [];
        }
    }

    if (validHighAccuracyComps($accuracyScore, count($matchedCompsIds))) {
        $data = getCalculatedValues($matchedCompsIds, $accuracyScore, $sqft, $listPrice, $structureTypes, $address);

         APIHelper::SendResponse($data, 1);
        exit();

    } else {
        $previousMatchIdsCount = count($matchedCompsIds);
        $totalAccuracyScore = count($matchedCompsIds) * 9.9;
        $maximumRecordCheckCount = 10;
        $i = 1;
        foreach ($compsResults as $compsItem) {
            if (isBetween80To120Percentage($sqft, $compsItem) && (($i + $previousMatchIdsCount) <= $maximumRecordCheckCount)) {
                if (!in_array($compsItem['id'], $matchedCompsIds)) {
                    $matchedCompsIds[] = $compsItem['id'];
                    $totalAccuracyScore += 7.9;
                    $i++;
                }
            }
        }

        $previousMatchIdsCount = count($matchedCompsIds);
        if ($previousMatchIdsCount < $maximumRecordCheckCount) {
            $i = 1;
            foreach ($compsResults as $compsItem) {
                if ($lat != null && isWithInMile($lat, $lng, $compsItem['latitude'], $compsItem['longitude']) && (($i + $previousMatchIdsCount) <= $maximumRecordCheckCount)) {
                    if (!in_array($compsItem['id'], $matchedCompsIds)) {
                        $matchedCompsIds[] = $compsItem['id'];
                        $totalAccuracyScore += 5.9;
                        $i++;
                    }
                }
            }
        }

        $previousMatchIdsCount = count($matchedCompsIds);
        if ($previousMatchIdsCount < $maximumRecordCheckCount) {
            $i = 1;
            foreach ($compsResults as $compsItem) {
                if (!in_array($compsItem['structure_type'], $structureTypes) && (($i + $previousMatchIdsCount) <= $maximumRecordCheckCount)) {
                    if (!in_array($compsItem['id'], $matchedCompsIds)) {
                        $matchedCompsIds[] = $compsItem['id'];
                        $totalAccuracyScore += 2.9;
                        $i++;
                    }
                }
            }
        }

        $finalAccuracy = 0;
        if (count($matchedCompsIds) != 0) {
            $finalAccuracy = round($totalAccuracyScore / count($matchedCompsIds), 2);
        }
        $data = getCalculatedValues($matchedCompsIds, $finalAccuracy, $sqft, $listPrice, $structureTypes, $address);


         APIHelper::SendResponse($data, 1);
        exit();

    }
}
 APIHelper::SendResponse([], 0, 'Invalid Data');
exit();


function getStructureTypes($zillowPropertyType, $address): array
{
    global $db;
    $structureType = null;
    if ($address != null) {
        $query = pg_query_params($db, "SELECT structure_type FROM properties 
                      WHERE clean_address like lower(translate($1, ',',''))", ["%" . $address . '%']);
        $structureType = pg_fetch_assoc($query) ?? null;
        $structureType = $structureType['structure_type'];
    }
    if ($structureType != null) {
        return [$structureType];
    }

    $propertyMapping = [
//        'SINGLE_FAMILY' => ['Detached', 'Manufactured', 'Mobile Pre 1976'],
//        'TOWNHOUSE' => ['End of Row/Townhouse', 'Interior Row/Townhouse'],
//        'MULTI_FAMILY' => ['Twin/Semi-Detached'],
//        'CONDO' => ['Unit/Flat/Apartment', 'Penthouse Unit/Flat/Apartment'],
//        'LOT' => ['Garage/Parking Space', 'Other'],
//        'APARTMENT' => ['Penthouse Unit/Flat/Apartment', 'Unit/Flat/Apartment'],
//        'UNKNOWN' => ['Detached'],
//
        'SINGLE_FAMILY' => ['Detached'],
        'TOWNHOUSE' => ['End of Row/Townhouse', 'Interior Row/Townhouse'],
        'MULTI_FAMILY' => ['NONE'],
        'CONDO' => ['Unit/Flat/Apartment', 'Penthouse Unit/Flat/Apartment'],
        'LOT' => ['Garage/Parking Space', 'Other'],
        'APARTMENT' => ['Penthouse Unit/Flat/Apartment', 'Unit/Flat/Apartment'],
        'UNKNOWN' => ['Detached']
    ];

    return $propertyMapping[$zillowPropertyType] ?? ['Detached'];
}

function getActiveListing($address)
{
    global $db;
    if ($address != null) {
        $query = pg_query_params($db, "SELECT id, list_price, listing_entry_date FROM properties 
                                          WHERE clean_address like lower(translate($1, ',','')) AND status = 'Active'", ["%" . $address . '%']);
        $res = pg_fetch_assoc($query) ?? null;
        return $res;
    }
    return null;
}

function getMileRangePropertiesIds($filteredProperties, $lat, $lng)
{
    $matchedProperties = [];
    foreach ($filteredProperties as $avItem) {
        if ($avItem['latitude'] != null) {
            $withInMile = isWithInMile($lat, $lng, $avItem['latitude'], $avItem['longitude']);
            if ($withInMile) {
                $matchedProperties[] = $avItem['id'];
            }
        }
    }

    return array_filter($matchedProperties);
}

function getSqftRangePropertiesIds($filteredProperties, $sqft, $matchedItems = [])
{
    $matchedProperties = [];
    foreach ($filteredProperties as $avItem) {
        if (!empty($matchedItems) && in_array($avItem['id'], $matchedItems)) {
            if (isBetween80To120Percentage($sqft, $avItem)) {
                $matchedProperties[] = $avItem['id'];
            }
        }
    }

    return array_filter($matchedProperties);
}

function isWithInMile($subLat, $subLong, $lat, $long)
{
    $diffMile = GetGeoDistance($subLat, $subLong, $lat, $long);
    if ($diffMile > 1) {
        return false;
    }

    return true;
}

function isBetween80To120Percentage($sqft, $filteredProperty)
{
    $subjectTFSqftMin = ($sqft / 100) * 80;
    $subjectTFSqftMax = ($sqft / 100) * 120;

    if ($subjectTFSqftMin <= $filteredProperty['total_finished_sqft'] && $subjectTFSqftMax >= $filteredProperty['total_finished_sqft']) {
        return true;
    }

    return false;
}

function validHighAccuracyComps($accuracyScore, $compsCount)
{
    if ($accuracyScore == 9.9 && $compsCount >= 10) {
        return true;
    }

    return false;
}


function getCalculatedValues($matchedProperties, $accuracyScore, $sqft, $listPrice, $structureTypes, $address)
{
    global $db;

    $resAvgArv = 0;
    $resAvgAvm = 0;
    $resAvgPriceSqft = 0;
    $resAvgSales = 0;
    $resAvgDeltaPSF = 0;
    $resAvgDom = 0;
    $resAvgHighCompCluster = 0;
    $resAvgLowCompCluster = 0;
    $resAvgHighCompPPSF = 0;
    $resAvgLowCompPPSF = 0;
    $resAvgNonConformingSales = 0;
    $resAvgSqft = 0;

    $resPricePerSqft = 0;
    $resFullRehab = 0;

    $resAccuracyScore = is_nan($accuracyScore) ? 0 : $accuracyScore;

    $pgArrayIds = '{' . implode(', ', $matchedProperties) . '}';
    $avgProcessQuery = pg_query_params($db, "WITH filtered_props AS (
    SELECT *
    FROM properties
    WHERE id = ANY($1)
),
ranks AS (
    SELECT p.*,
           row_number() OVER (ORDER BY close_price DESC)           AS close_price_rank,
           row_number() OVER (ORDER BY price_per_sqft_closed DESC) AS price_per_sqft_rank,
           row_number() OVER (ORDER BY close_price ASC)            AS close_price_low_rank,
           row_number() OVER (ORDER BY price_per_sqft_closed ASC)  AS price_per_sqft_low_rank
    FROM filtered_props p
),
avg_vals AS (
    SELECT avg(price_per_sqft_closed) AS avg_p_sqft
    FROM ranks
)
SELECT 
    round(avg(r.close_price), 2)                         AS average_c_price,
    round(avg(r.list_price), 2)                          AS avg_list,
    round(avg(r.price_per_sqft_closed), 2)               AS avg_p_sqft,
    round(avg(r.dom), 2)                                 AS avg_dom,
    round(avg(r.total_finished_sqft), 2)                 AS avg_sqft,
    count(*)                                             AS sales,
    round(avg(CASE WHEN r.close_price_rank <= 3 THEN r.close_price END), 2) AS high_comp_cluster,
    round(avg(CASE WHEN r.price_per_sqft_rank <= 3 THEN r.price_per_sqft_closed END), 2) AS high_comp_ppsf,
    round(avg(CASE WHEN r.close_price_low_rank <= 3 THEN r.close_price END), 2) AS low_comp_cluster,
    round(avg(CASE WHEN r.price_per_sqft_low_rank <= 3 THEN r.price_per_sqft_closed END), 2) AS low_comp_ppsf,
    count(*) FILTER (
        WHERE r.price_per_sqft_closed > (SELECT 3 * avg_p_sqft FROM avg_vals)
    ) AS non_conforming_sales
FROM ranks r;
", [$pgArrayIds]);
    $avgProcessResult = pg_fetch_all($avgProcessQuery);
    if (!empty($avgProcessResult)) {
        $resAvgClosedPrice = $avgProcessResult[0]['average_c_price'];
        $avg_list = $avgProcessResult[0]['avg_list'];
        $resAvgPriceSqft = $avgProcessResult[0]['avg_p_sqft'];
        $resAvgDom = $avgProcessResult[0]['avg_dom'];
        $resAvgSqft = $avgProcessResult[0]['avg_sqft'];
        $resAvgSales = $avgProcessResult[0]['sales'];
        $resAvgHighCompCluster = $avgProcessResult[0]['high_comp_cluster'];
        $resAvgHighCompPPSF = $avgProcessResult[0]['high_comp_ppsf'];
        $resAvgLowCompCluster = $avgProcessResult[0]['low_comp_cluster'];
        $resAvgLowCompPPSF = $avgProcessResult[0]['low_comp_ppsf'];
        $resAvgNonConformingSales = $avgProcessResult[0]['non_conforming_sales'];

        $resAvgAvm = ($sqft * $resAvgPriceSqft);
        $resAvgArv = ($sqft * $resAvgHighCompPPSF);

        $resFullRehab = ($sqft * 50);
        $resPricePerSqft = ($sqft != 0) ? $listPrice / $sqft : 0;

        $resAvgDeltaPSF = 0;
        if (!empty($resPricePerSqft) && !empty($resPricePerSqft) && !empty($sqft)) {
            $resAvgDeltaPSF = ($resAvgPriceSqft - $resPricePerSqft);
        }

//        $total_debits = ($resAvgArv * 0.07);
//        $debt_service = ((($listPrice + $full_rehab) * 0.9 * (0.1 / 12)) / (1 - pow(1 + (0.1 / 12), -360))) * 6;
    }

    return [
        'average_data' => [
            'arv' => floatval($resAvgArv),
            'avm' => floatval($resAvgAvm),
            'price_per_sqft' => floatval($resAvgPriceSqft),
            'sales' => floatval($resAvgSales),
            'delta_psf' => floatval($resAvgDeltaPSF),
            'dom' => floatval($resAvgDom),
            'high_comps_cluster' => floatval($resAvgHighCompCluster),
            'low_comps_cluster' => floatval($resAvgLowCompCluster),
            'high_comp_ppsf' => floatval($resAvgHighCompPPSF),
            'low_comp_ppsf' => floatval($resAvgLowCompPPSF),
            'non_conforming_sales' => floatval($resAvgNonConformingSales),
            'sqft' => floatval($resAvgSqft),
            'accuracy_score' => floatval($resAccuracyScore),
        ],
        'subject_data' => [
            'price_per_sqft' => floatval($resPricePerSqft),
            'full_rehab' => floatval($resFullRehab),

        ],
        'structure_type' => $structureTypes,
        'active_listing' => getActiveListing($address)
    ];
}