<?php
ini_set('memory_limit', '4G');
include_once "mysql_conn.php";
include_once "../includes/APIHelper.php";

APIHelper::Authentication();

global $dsn, $username, $password;
try {
    $conn = new PDO($dsn, $username, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $reqAddress = APIHelper::GetParam('address', true);
    $reqZip = APIHelper::GetParam('zip', true);
    $reqLat = APIHelper::GetParam('lat', true);
    $reqLng = APIHelper::GetParam('lng', true);
    $reqRange = APIHelper::GetParam('range', true);
    $reqDays = APIHelper::GetParam('days', true);
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

    if (empty($zipCode)) {
         APIHelper::SendResponse(null, 0, 'No records found');
        die();
    }


    $stmt = $conn->prepare("

WITH investor_base AS (
  SELECT 
    COALESCE(p.OwnerNAME1FULL, p.MailingFullStreetAddress) AS investor_identifier,
    p.MailingFullStreetAddress,
    p.MailingCity,
    p.MailingState,
    p.MailingZIP5,
    p.SitusLatitude,
    p.SitusLongitude,
    p.CurrentSaleRecordingDate,
    p.CurrentSalesPrice,
    p.OwnerOccupied,
    p.Owner1CorpInd,
    p.OwnerNAME1FULL,
    p.ConcurrentMtg1LoanAmt,
    p.FIPS,
    p.SitusFullStreetAddress,
    p.SitusCity,
    p.SitusState,
    p.SitusZIP5,
    p.Bedrooms,
    p.BathFull,
    p.SumLivingAreaSqFt,
    p.BuildingClassCode,
    
    CASE 
      WHEN p.OwnerOccupied = 'N' OR p.SitusState != p.MailingState THEN 1 ELSE 0
    END AS is_absentee,
    CASE 
      WHEN p.Owner1CorpInd = 'Y' 
         OR p.OwnerNAME1FULL LIKE '%LLC%' 
         OR p.OwnerNAME1FULL LIKE '%INC%' 
         OR p.OwnerNAME1FULL LIKE '%CORP%' 
         OR p.OwnerNAME1FULL LIKE '%PARTNERSHIP%' 
      THEN 1 ELSE 0 
    END AS is_llc,
    CASE 
      WHEN p.ConcurrentMtg1LoanAmt IS NULL OR p.ConcurrentMtg1LoanAmt = 0 THEN 1 ELSE 0
    END AS is_cash,
    CASE 
      WHEN p.SitusZIP5 = :zip_code THEN 1 ELSE 0
    END AS is_folsom
  FROM datatree_property p
  WHERE 
    STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') IS NOT NULL
    AND STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
    AND p.OwnerNAME1FULL NOT LIKE '%PULTE%'
    AND p.OwnerNAME1FULL NOT LIKE '%HORTON%'
    AND p.FIPS = :fips
    AND (
      3959 * ACOS(
        COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
        COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
        SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
      )
    ) <= :radius_miles
),
recent_sales AS (
  SELECT *
  FROM (
    SELECT 
      ib.*,
      ROW_NUMBER() OVER (PARTITION BY investor_identifier ORDER BY STR_TO_DATE(ib.CurrentSaleRecordingDate, '%Y-%m-%d') DESC) AS rn
    FROM investor_base ib
  ) ranked
  WHERE rn = 1
),
aggregated_investors AS ( 
  SELECT 
    ib.investor_identifier,
    ib.MailingFullStreetAddress,
    ib.MailingCity,
    ib.MailingState,
    ib.MailingZIP5,
    rs.CurrentSaleRecordingDate as mrp_purchase,
    rs.CurrentSalesPrice as mrp_sales_price,
    rs.SitusFullStreetAddress as mrp_fullstreet,
    rs.SitusCity as mrp_city,
    rs.SitusState as mrp_state,
    rs.SitusZIP5 as mrp_zip,
    rs.Bedrooms as mrp_beds,
    rs.BathFull as mrp_bath,
    rs.SumLivingAreaSqFt as mrp_sqft,
    rs.BuildingClassCode as mrp_type_class,
    rs.SitusLatitude as mrp_lat,
    rs.SitusLongitude as mrp_lng,
    
    COUNT(*) AS properties_owned,
    SUM(ib.is_absentee) AS absentee_count,
    MAX(ib.is_llc) AS is_llc,
    SUM(ib.is_folsom) AS folsom_properties,
    SUM(ib.is_cash) AS cash_purchase_count,
    (
      (SUM(ib.is_folsom) * 5) +
      (CASE 
         WHEN rs.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 10
         WHEN rs.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 5
         ELSE 2
       END) +
      (LEAST(COUNT(*), 10) * 2) +
      (SUM(ib.is_cash) * 3) +
      (MAX(ib.is_llc) * 5) +
      (SUM(ib.is_absentee) * 1)
    ) AS likelihood_score,
    ib.FIPS
  FROM investor_base ib
  JOIN recent_sales rs ON ib.investor_identifier = rs.investor_identifier
  GROUP BY 
    ib.investor_identifier,
    ib.MailingFullStreetAddress,
    ib.MailingCity,
    ib.MailingState,
    ib.MailingZIP5,
    rs.CurrentSaleRecordingDate,
    ib.FIPS
),
ranked_investors AS (
  SELECT *,
         RANK() OVER (ORDER BY likelihood_score DESC) AS rank_position
  FROM aggregated_investors
)
SELECT 
  investor_identifier,
  MailingFullStreetAddress,
  MailingCity,
  MailingState,
  MailingZIP5,
  properties_owned,
  mrp_purchase,
  mrp_sales_price,
  mrp_fullstreet,
  mrp_city,
  mrp_state,
  mrp_zip,
  mrp_beds,
  mrp_bath,
  mrp_sqft,
  mrp_type_class,
  mrp_lat,
  mrp_lng,
  absentee_count,
  is_llc,
  folsom_properties,
  cash_purchase_count,
  likelihood_score,
  FIPS,
  CASE WHEN rank_position <= 3 THEN 'Most Likely Buyer' ELSE '' END AS most_likely_buyer_tag
FROM ranked_investors
WHERE 
  properties_owned > 1
  AND (absentee_count > 0 OR is_llc = 1)
ORDER BY 
  likelihood_score DESC,
  folsom_properties DESC,
  properties_owned DESC,
  mrp_purchase DESC
LIMIT 50
");


    $stmt->bindParam(":zip_code", $zipCode);
    $stmt->bindParam(":fips", $reqFips);
    $stmt->bindParam(":center_lat", $reqLat);
    $stmt->bindParam(":center_lng", $reqLng);
    $stmt->bindParam(":radius_miles", $reqRange);
    $stmt->bindParam(":days", $reqDays);
    $stmt->execute();

    $resultArray = [];
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $resultArray[] = $row;
    }

     APIHelper::SendResponse($resultArray, 1);
    die();


} catch (PDOException $e) {
    echo "Query failed: " . $e->getMessage();
}
