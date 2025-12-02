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
    $reqCity = APIHelper::GetParam('city', true);
    $reqState = APIHelper::GetParam('state', true);
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

    $normalizedAddressQuery = normalizeQueryWhereClause();
    $stmt = $conn->prepare("SELECT 
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
     $normalizedAddressQuery
  AND 
      lower(SitusCity) = lower(:city) AND
      lower(SitusState) = lower(:state) AND
      lower(SitusZIP5) = lower(:zip) AND
       (:fips = '' OR lower(FIPS) = :fips)
ORDER BY PropertyID DESC 
LIMIT 10
");

    $normalizedAddress = normalizeStreetSuffix($reqAddress);
    $stmt->bindParam(":normalized_address", $normalizedAddress);
    $stmt->bindParam(":city", $reqCity);
    $stmt->bindParam(":state", $reqState);
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
