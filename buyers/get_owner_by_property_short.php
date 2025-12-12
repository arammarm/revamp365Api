<?php

ini_set('memory_limit', '4G');
include_once "mysql_conn.php";
include_once "../includes/APIHelper.php";

APIHelper::Authentication();

global $dsn, $username, $password;
try {
    $conn = new PDO($dsn, $username, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $reqStreet = APIHelper::GetParam('street', true);
    $reqCity = APIHelper::GetParam('city', true);
    $reqState = APIHelper::GetParam('state', true);
    $reqZip = APIHelper::GetParam('zip', true);
    $reqFips = APIHelper::GetParam('fips', true);

    // Validate required parameters - if missing, return empty result
    if (empty($reqStreet) || empty($reqCity) || empty($reqState) || empty($reqZip)) {
        APIHelper::SendResponse([], 0, 'No records found');
        die();
    }

    $normalizedAddressQuery = normalizeQueryWhereClause();
    $stmt = $conn->prepare("SELECT 
        MailingFullStreetAddress as mailing_full_street_address,
        MailingCity as mailing_city,
        MailingState as mailing_state,
        MailingZIP5 as mailing_zip5,
        OwnerNAME1FULL as owner_name_full,
        Bedrooms as bedrooms,
        BathFull as bath_full,
        YearBuilt as year_built,
        SumLivingAreaSqFt as sum_living_area_sqft,
        AssdTotalValue as assessed_value,
        (CASE 
            WHEN Mtg1LoanAmt IS NOT NULL THEN 1 ELSE 0 END +
         CASE 
            WHEN Mtg2LoanAmt IS NOT NULL THEN 1 ELSE 0 END +
         CASE 
            WHEN Mtg3LoanAmt IS NOT NULL THEN 1 ELSE 0 END +
         CASE 
            WHEN Mtg4LoanAmt IS NOT NULL THEN 1 ELSE 0 END) as mortgage_counts,
        (CASE 
            WHEN Mtg1LoanAmt IS NULL AND Mtg2LoanAmt IS NULL AND Mtg3LoanAmt IS NULL AND Mtg4LoanAmt IS NULL 
            THEN NULL 
            ELSE (COALESCE(Mtg1LoanAmt, 0) + COALESCE(Mtg2LoanAmt, 0) + COALESCE(Mtg3LoanAmt, 0) + COALESCE(Mtg4LoanAmt, 0))
         END) as mortgage_amount,
        CurrentSaleRecordingDate as last_sale_date,
        CurrentSalesPrice as last_sale_amount
    FROM datatree_property 
    WHERE 
          $normalizedAddressQuery AND
          lower(SitusCity) = lower(:city) AND
          lower(SitusState) = lower(:state) AND
          lower(SitusZIP5) = lower(:zip) AND
          (:fips = '' OR lower(FIPS) = :fips)
    ORDER BY PropertyID DESC 
    LIMIT 1
    ");

    $normalizedAddress = normalizeStreetSuffix($reqStreet);
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

