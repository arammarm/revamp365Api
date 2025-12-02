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

    $stmt = $conn->prepare("WITH base_properties AS (
  SELECT
    p.*,
    UPPER(TRIM(COALESCE(p.OwnerNAME1FULL, p.MailingFullStreetAddress))) AS investor_identifier,
    CASE 
      WHEN p.OwnerOccupied = 'N' OR p.SitusState != p.MailingState THEN 1 ELSE 0 
    END AS is_absentee,
    CASE 
      WHEN p.Owner1CorpInd IN ('Y','T','1')
           OR UPPER(p.OwnerNAME1FULL) LIKE '%LLC%'
           OR UPPER(p.OwnerNAME1FULL) LIKE '%INC%'
           OR UPPER(p.OwnerNAME1FULL) LIKE '%CORP%'
           OR UPPER(p.OwnerNAME1FULL) LIKE '% LP%'
           OR UPPER(p.OwnerNAME1FULL) LIKE '%PARTNER%'
      THEN 1 ELSE 0 
    END AS is_llc,
    CASE 
      WHEN p.ConcurrentMtg1LoanAmt IS NULL OR p.ConcurrentMtg1LoanAmt = 0 THEN 1 ELSE 0 
    END AS is_cash,
    CASE 
      WHEN p.SitusZIP5 = :zip_code THEN 1 ELSE 0 
    END AS is_local_zip,
    3959 * ACOS(
      COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
      COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
      SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
    ) AS distance_miles
  FROM datatree_property p
  WHERE
    p.FIPS = :fips
    AND p.CurrentSaleRecordingDate IS NOT NULL
    AND STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
    AND p.OwnerNAME1FULL NOT LIKE '%PULTE%'
    AND p.OwnerNAME1FULL NOT LIKE '%HORTON%'
    AND 3959 * ACOS(
      COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
      COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
      SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
    ) <= :radius_miles
),
investor_stats AS (
  SELECT
    investor_identifier,
    COUNT(*) AS properties_owned,
    MAX(CurrentSaleRecordingDate) AS most_recent_purchase,
    SUM(is_absentee) AS absentee_count,
    MAX(is_llc) AS is_llc,
    SUM(is_local_zip) AS local_zip_count,
    SUM(is_cash) AS cash_purchase_count,
    MAX(FIPS) AS FIPS
  FROM base_properties
  GROUP BY investor_identifier
),
investor_properties AS (
  SELECT
    investor_identifier,
    CurrentSaleRecordingDate AS mrp_purchase,
    MIN(MailingFullStreetAddress) AS MailingFullStreetAddress,
    MIN(MailingCity) AS MailingCity,
    MIN(MailingState) AS MailingState,
    MIN(MailingZIP5) AS MailingZIP5,
    MIN(SitusFullStreetAddress) AS mrp_fullstreet,
    MIN(SitusCity) AS mrp_city,
    MIN(SitusState) AS mrp_state,
    MIN(SitusZIP5) AS mrp_zip,
    MIN(CurrentSalesPrice) AS mrp_sales_price,
    MIN(Bedrooms) AS mrp_beds,
    MIN(BathFull) AS mrp_bath,
    MIN(SumLivingAreaSqFt) AS mrp_sqft,
    MIN(BuildingClassCode) AS mrp_type_class,
    MIN(SitusLatitude) AS mrp_lat,
    MIN(SitusLongitude) AS mrp_lng
  FROM base_properties
  GROUP BY investor_identifier, CurrentSaleRecordingDate
),
final_joined AS (
  SELECT
    s.*,
    p.MailingFullStreetAddress,
    p.MailingCity,
    p.MailingState,
    p.MailingZIP5,
    p.mrp_purchase,
    p.mrp_sales_price,
    p.mrp_fullstreet,
    p.mrp_city,
    p.mrp_state,
    p.mrp_zip,
    p.mrp_beds,
    p.mrp_bath,
    p.mrp_sqft,
    p.mrp_type_class,
    p.mrp_lat,
    p.mrp_lng,
    (
      (local_zip_count * 8)
      + CASE
          WHEN most_recent_purchase >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 20
          WHEN most_recent_purchase >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 10
          ELSE 4
        END
      + (CASE WHEN properties_owned > 5 THEN 5 ELSE properties_owned END) * 3
      + (cash_purchase_count * 1)
      + (is_llc * 5)
      + (absentee_count * 1)
    ) AS likelihood_score
  FROM investor_stats s
  JOIN investor_properties p
    ON p.investor_identifier = s.investor_identifier
   AND p.mrp_purchase = s.most_recent_purchase
),
ranked_investors AS (
  SELECT *,
    RANK() OVER (ORDER BY likelihood_score DESC) AS rank_position
  FROM final_joined
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
  local_zip_count AS folsom_properties,
  cash_purchase_count,
  likelihood_score,
  FIPS,
  rank_position,
  CASE WHEN rank_position <= 3 THEN 'Most Likely Buyer' ELSE '' END AS most_likely_buyer_tag
FROM ranked_investors
WHERE
  (properties_owned > 1 OR (local_zip_count > 0 AND most_recent_purchase >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)))
  AND (absentee_count > 0 OR is_llc = 1 OR local_zip_count > 0)
ORDER BY
  likelihood_score DESC,
  properties_owned DESC,
  most_recent_purchase DESC
LIMIT 100;");

//    $stmt = $conn->prepare("WITH base_properties AS (
//  SELECT
//    p.*,
//    UPPER(TRIM(COALESCE(p.OwnerNAME1FULL, p.MailingFullStreetAddress))) AS investor_identifier,
//    CASE
//      WHEN p.OwnerOccupied = 'N' OR p.SitusState != p.MailingState THEN 1 ELSE 0
//    END AS is_absentee,
//    CASE
//      WHEN p.Owner1CorpInd IN ('Y','T','1')
//           OR UPPER(p.OwnerNAME1FULL) LIKE '%LLC%'
//           OR UPPER(p.OwnerNAME1FULL) LIKE '%INC%'
//           OR UPPER(p.OwnerNAME1FULL) LIKE '%CORP%'
//           OR UPPER(p.OwnerNAME1FULL) LIKE '% LP%'
//           OR UPPER(p.OwnerNAME1FULL) LIKE '%PARTNER%'
//      THEN 1 ELSE 0
//    END AS is_llc,
//    CASE
//      WHEN p.ConcurrentMtg1LoanAmt IS NULL OR p.ConcurrentMtg1LoanAmt = 0 THEN 1 ELSE 0
//    END AS is_cash,
//    CASE
//      WHEN p.SitusZIP5 = :zip_code THEN 1 ELSE 0
//    END AS is_local_zip,
//    3959 * ACOS(
//      COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
//      COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
//      SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
//    ) AS distance_miles
//  FROM datatree_property p
//  WHERE
//    p.FIPS = :fips
//    AND p.CurrentSaleRecordingDate IS NOT NULL
//    AND STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
//    AND p.OwnerNAME1FULL NOT LIKE '%PULTE%'
//    AND p.OwnerNAME1FULL NOT LIKE '%HORTON%'
//    AND 3959 * ACOS(
//      COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
//      COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
//      SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
//    ) <= :radius_miles
//),
//recent_sales AS (
//  SELECT
//    *,
//    ROW_NUMBER() OVER (PARTITION BY investor_identifier ORDER BY STR_TO_DATE(CurrentSaleRecordingDate, '%Y-%m-%d') DESC) AS rn
//  FROM base_properties
//),
//latest_sales AS (
//  SELECT * FROM recent_sales WHERE rn = 1
//),
//aggregated_investors AS (
//  SELECT
//    bp.investor_identifier,
//    MIN(bp.MailingFullStreetAddress) AS MailingFullStreetAddress,
//    MIN(bp.MailingCity) AS MailingCity,
//    MIN(bp.MailingState) AS MailingState,
//    MIN(bp.MailingZIP5) AS MailingZIP5,
//    ls.CurrentSaleRecordingDate AS mrp_purchase,
//    ls.CurrentSalesPrice AS mrp_sales_price,
//    ls.SitusFullStreetAddress AS mrp_fullstreet,
//    ls.SitusCity AS mrp_city,
//    ls.SitusState AS mrp_state,
//    ls.SitusZIP5 AS mrp_zip,
//    ls.Bedrooms AS mrp_beds,
//    ls.BathFull AS mrp_bath,
//    ls.SumLivingAreaSqFt AS mrp_sqft,
//    ls.BuildingClassCode AS mrp_type_class,
//    ls.SitusLatitude AS mrp_lat,
//    ls.SitusLongitude AS mrp_lng,
//    COUNT(*) AS properties_owned,
//    SUM(bp.is_absentee) AS absentee_count,
//    MAX(bp.is_llc) AS is_llc,
//    SUM(bp.is_local_zip) AS folsom_properties,
//    SUM(bp.is_cash) AS cash_purchase_count,
//    bp.FIPS,
//    (
//      (SUM(bp.is_local_zip) * 8)
//      + CASE
//          WHEN ls.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 20
//          WHEN ls.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 10
//          ELSE 4
//        END
//      + (CASE WHEN COUNT(*) > 5 THEN 5 ELSE COUNT(*) END) * 3
//      + (SUM(bp.is_cash) * 1)
//      + (MAX(bp.is_llc) * 5)
//      + (SUM(bp.is_absentee) * 1)
//    ) AS likelihood_score
//  FROM base_properties bp
//  JOIN latest_sales ls ON bp.investor_identifier = ls.investor_identifier
//  GROUP BY
//    bp.investor_identifier,
//    ls.CurrentSaleRecordingDate,
//    bp.FIPS,
//    ls.CurrentSalesPrice,
//    ls.SitusFullStreetAddress,
//    ls.SitusCity,
//    ls.SitusState,
//    ls.SitusZIP5,
//    ls.Bedrooms,
//    ls.BathFull,
//    ls.SumLivingAreaSqFt,
//    ls.BuildingClassCode,
//    ls.SitusLatitude,
//    ls.SitusLongitude
//),
//ranked_investors AS (
//  SELECT
//    *,
//    RANK() OVER (ORDER BY likelihood_score DESC) AS rank_position
//  FROM aggregated_investors
//)
//SELECT
//  investor_identifier,
//  MailingFullStreetAddress,
//  MailingCity,
//  MailingState,
//  MailingZIP5,
//  properties_owned,
//  mrp_purchase,
//  mrp_sales_price,
//  mrp_fullstreet,
//  mrp_city,
//  mrp_state,
//  mrp_zip,
//  mrp_beds,
//  mrp_bath,
//  mrp_sqft,
//  mrp_type_class,
//  mrp_lat,
//  mrp_lng,
//  absentee_count,
//  is_llc,
//  folsom_properties,
//  cash_purchase_count,
//  likelihood_score,
//  FIPS,
//  CASE WHEN rank_position <= 3 THEN 'Most Likely Buyer' ELSE '' END AS most_likely_buyer_tag
//FROM ranked_investors
//WHERE
//  properties_owned > 1
//  AND (absentee_count > 0 OR is_llc = 1 OR folsom_properties > 0)
//ORDER BY
//  likelihood_score DESC,
//  folsom_properties DESC,
//  properties_owned DESC,
//  mrp_purchase DESC
//LIMIT 100;");


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
