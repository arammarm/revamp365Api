<?php
ini_set('memory_limit', '4G');
include_once "mysql_conn.php";
include_once "../includes/APIHelper.php";

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

    // More precise address matching with exact house number validation
    $query = "SELECT 
PropertyID,
OwnerNAME1FULL,
OwnerNAME2FULL,
MailingFullStreetAddress,
MailingCity,
MailingState,
MailingZIP5,
ConcurrentMtg1LoanAmt,
ConcurrentMtg1Lender,
ConcurrentMtg1Term,
ConcurrentMtg2LoanAmt,
ConcurrentMtg2Lender,
ConcurrentMtg2Term,
ConcurrentMtg1InterestRate,
SitusFullStreetAddress,
SitusCity, 
SitusState,
SitusZIP5
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
LIMIT 10
";

    $stmt = $conn->prepare($query);

    $stmt->bindParam(":house_number", $houseNumber);
    $stmt->bindParam(":normalized_street", $normalizedStreetName);
    $stmt->bindParam(":original_street", $streetName);
    $stmt->bindParam(":zip", $reqZip);
    $stmt->bindParam(":fips", $reqFips);
    
    $stmt->execute();

    $resultArray = [];
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $resultArray[] = $row;
    }

    APIHelper::SendResponse($resultArray, 1);
    die();


} catch (PDOException $e) {
    $error = "Query failed: " . $e->getMessage();
    APIHelper::SendResponse([], 0, $error);
}
