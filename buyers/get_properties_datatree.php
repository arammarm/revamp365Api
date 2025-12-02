<?php

ini_set('memory_limit', '4G');
include_once "mysql_conn.php";
include_once "../includes/APIHelper.php";
include_once "../includes/DatatreeAPIHelper.php";
include_once "../includes/DatatreeAPIHelperExtended.php";

APIHelper::Authentication();

global $dsn, $username, $password;
try {
    $conn = new PDO($dsn, $username, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $reqAddress = APIHelper::GetParam('full_street_address', true);
    $reqZip = APIHelper::GetParam('zip', true);
    $reqFips = APIHelper::GetParam('fips', true);


    $zipCode = null;
    if ($reqZip == '') {
        preg_match_all('/\b\d{5}\b/', $reqAddress, $matches);
        if (!empty($matches[0])) {
            $zipCode = end($matches[0]);
        } else {
            echo "No ZIP Code found";
        }
    } else {
        $zipCode = $reqZip;
    }

    if (empty($zipCode) || empty($reqAddress)) {
        APIHelper::SendResponse(null, 0, 'No records found');
        die();
    }

    // Extract house number from the address
    preg_match('/^(\d+)/', trim($reqAddress), $houseNumberMatches);
    $houseNumber = isset($houseNumberMatches[1]) ? $houseNumberMatches[1] : '';
    
    // Validate that we have a house number
    if (empty($houseNumber)) {
        APIHelper::SendResponse(null, 0, 'Invalid address format - house number required');
        die();
    }
    
    // Extract street name (everything after house number)
    $streetName = trim(preg_replace('/^\d+\s*/', '', $reqAddress));
    $normalizedStreetName = normalizeStreetSuffix($streetName);

    // First, try exact match
    $exactQuery = "SELECT *
FROM datatree_property 
WHERE 
    SitusHouseNbr = :house_number AND
    (lower(SitusStreet) LIKE CONCAT('%', :normalized_street, '%') 
     OR lower(SitusStreet) LIKE CONCAT('%', :original_street, '%')
     OR lower(SitusFullStreetAddress) LIKE CONCAT('%', :normalized_street, '%')
     OR lower(SitusFullStreetAddress) LIKE CONCAT('%', :original_street, '%')) AND
    SitusZIP5 = :zip AND
    (:fips = '' OR FIPS = :fips)
ORDER BY PropertyID DESC 
LIMIT 1";

    $stmt = $conn->prepare($exactQuery);
    $stmt->bindParam(":house_number", $houseNumber);
    $stmt->bindParam(":normalized_street", $normalizedStreetName);
    $stmt->bindParam(":original_street", $streetName);
    $stmt->bindParam(":zip", $zipCode);
    $stmt->bindParam(":fips", $reqFips);
    $stmt->execute();

    $resultArray = [];
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $transformedRow = DatatreeAPIHelper::transformPropertyData($row);
        $resultArray[] = $transformedRow;
    }

    // If no exact match found, try fuzzy matching
    if (empty($resultArray)) {
        $fuzzyQuery = "SELECT *
    FROM datatree_property 
    WHERE 
        SitusHouseNbr = :house_number AND
        (lower(SitusStreet) LIKE CONCAT('%', :normalized_street, '%') 
         OR lower(SitusStreet) LIKE CONCAT('%', :original_street, '%')
         OR lower(SitusFullStreetAddress) LIKE CONCAT('%', :normalized_street, '%')
         OR lower(SitusFullStreetAddress) LIKE CONCAT('%', :original_street, '%')) AND
        SitusZIP5 = :zip AND
        (:fips = '' OR FIPS = :fips)
    ORDER BY PropertyID DESC 
    LIMIT 1";
        
        $stmt = $conn->prepare($fuzzyQuery);
        $stmt->bindParam(":house_number", $houseNumber);
        $stmt->bindParam(":normalized_street", $normalizedStreetName);
        $stmt->bindParam(":original_street", $streetName);
        $stmt->bindParam(":zip", $zipCode);
        $stmt->bindParam(":fips", $reqFips);
        $stmt->execute();
        
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $transformedRow = DatatreeAPIHelper::transformPropertyData($row);
            $resultArray[] = $transformedRow;
        }
    }

    APIHelper::SendResponse($resultArray, 1);
    die();

} catch (PDOException $e) {
    $error = "Query failed: " . $e->getMessage();
    APIHelper::SendResponse([], 0, $error);
}
