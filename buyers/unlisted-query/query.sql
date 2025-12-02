-- ===================================================================
-- PRODUCTION QUERY - Use with PHP Prepared Statements
-- ===================================================================
-- Replace :parameter_name with actual values in PHP using PDO
-- This is a TEMPLATE - PHP will handle the parameter binding
-- ===================================================================

SELECT
  p.PropertyID,
  p.SitusLatitude,
  p.SitusLongitude,
  p.FIPS,
  p.APN,
  p.SitusFullStreetAddress,
  p.SitusCity,
  p.SitusState,
  p.SitusZIP5,
  p.SchoolDistrictName,
  p.Owner1LastName,
  p.Owner1FirstName,
  p.Owner2LastName,
  p.Owner2FirstName,
  p.OwnerNAME1FULL,
  p.OwnerNAME2FULL,
  p.Owner1CorpInd,
  p.Owner2CorpInd,
  p.OwnerOccupied,
  p.MailingFullStreetAddress,
  p.MailingCity,
  p.MailingState,
  p.MailingZIP5,
  p.AssdTotalValue,
  p.AssdLandValue,
  p.AssdImprovementValue,
  p.MarketTotalValue,
  p.TaxAmt,
  p.BuildingArea,
  p.LotSizeSqFt,
  p.LotSizeAcres,
  p.YearBuilt,
  p.Bedrooms,
  p.BathTotalCalc,
  p.StoriesNbrCode,
  p.CurrentSaleRecordingDate,
  p.CurrentSalesPrice,
  p.ConcurrentMtg1LoanAmt,
  p.ConcurrentMtg1Term,
  p.ConcurrentMtg1InterestRate,
  p.ConcurrentMtg1Lender,
  p.ConcurrentMtg1AdjOrFix,
  p.ConcurrentMtg1LoanType,
  p.ConcurrentMtg1RecordingDate,
  p.ConcurrentMtg2LoanAmt,
  p.ConcurrentMtg2Lender,
  p.TotalOpenLienNbr,
  p.TotalOpenLienAmt,
  p.IsListedFlag,
  p.IsListedFlagDate,
  p.IsListedPriceRange,
  p.StyleCode,
  
  -- Calculated Fields
  CASE 
    WHEN p.CurrentSaleRecordingDate IS NOT NULL THEN 
      YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate)
    ELSE NULL 
  END AS YearsOfOwnership,
  
  CASE 
    WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 THEN
      ROUND(((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) * 100, 2)
    WHEN p.MarketTotalValue > 0 AND (p.TotalOpenLienAmt IS NULL OR p.TotalOpenLienAmt = 0) THEN
      100.00
    ELSE NULL
  END AS EquityPercent,
  
  CASE 
    WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 THEN
      p.MarketTotalValue - p.TotalOpenLienAmt
    WHEN p.MarketTotalValue > 0 AND (p.TotalOpenLienAmt IS NULL OR p.TotalOpenLienAmt = 0) THEN
      p.MarketTotalValue
    ELSE NULL
  END AS EquityValue,
  
  CASE 
    WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 THEN
      ROUND((p.TotalOpenLienAmt / p.MarketTotalValue) * 100, 2)
    ELSE 0.00
  END AS LTV_Percent,
  
  -- MOTIVATION TAGS (47 tags)
  CASE WHEN p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL THEN 'Y' ELSE 'N' END AS No_Mortgage,
  CASE WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) < 0 THEN 'Y' ELSE 'N' END AS Negative_Equity,
  CASE WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) BETWEEN 0 AND 0.20 THEN 'Y' ELSE 'N' END AS Low_Equity,
  CASE WHEN (p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL) THEN 'Y' ELSE 'N' END AS High_Equity,
  CASE WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND (p.TotalOpenLienAmt / p.MarketTotalValue) >= 0.80 THEN 'Y' ELSE 'N' END AS High_LTV,
  CASE WHEN p.ConcurrentMtg1LoanType = '32' AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR) THEN 'Y' ELSE 'N' END AS Reverse_Mortgage,
  CASE WHEN (p.ConcurrentMtg1AdjOrFix = 'ADJ' OR p.ConcurrentMtg1LoanType = '3') AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) THEN 'Y' ELSE 'N' END AS ARM,
  CASE WHEN (p.IsListedFlag = 'Y' OR p.IsListedFlag = '1') AND (p.IsListedFlagDate IS NULL OR p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)) THEN 'Y' ELSE 'N' END AS On_Market,
  CASE WHEN p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH) AND (p.CurrentSaleRecordingDate IS NULL OR p.CurrentSaleRecordingDate < p.IsListedFlagDate) THEN 'Y' ELSE 'N' END AS Failed_Listing,
  CASE WHEN p.CurrentSalesPrice > 0 AND p.MarketTotalValue > 0 AND (p.CurrentSalesPrice / p.MarketTotalValue) < 0.85 AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH) THEN 'Y' ELSE 'N' END AS Below_Market_Sale,
  CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 'Y' ELSE 'N' END AS Recent_Sale,
  CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 'Y' ELSE 'N' END AS Historical_Sale,
  CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 15 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 5 YEAR) THEN 'Y' ELSE 'N' END AS Legacy_Sale,
  CASE WHEN p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 15 YEAR) THEN 'Y' ELSE 'N' END AS Ancient_Sale,
  CASE WHEN p.BuildingConditionCode IN (4,5) AND p.MarketTotalValue > 0 AND p.AssdLandValue > 0 AND (p.AssdLandValue / p.MarketTotalValue) >= 0.60 THEN 'Y' ELSE 'N' END AS Flip_Potential,
  CASE WHEN p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress THEN 'Y' ELSE 'N' END AS Absentee_Owner,
  CASE WHEN p.MailingState != p.SitusState THEN 'Y' ELSE 'N' END AS Out_Of_State_Owner,
  CASE WHEN (p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%' THEN 'Y' ELSE 'N' END AS Trust_Owned,
  CASE WHEN p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b' THEN 'Y' ELSE 'N' END AS Corporate_Owner,
  CASE WHEN (p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10) THEN 'Y' ELSE 'N' END AS Elderly_Owner,
  CASE WHEN p.OwnerOccupied = 'Y' THEN 'Y' ELSE 'N' END AS Owner_Occupied,
  CASE WHEN p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10) THEN 'Y' ELSE 'N' END AS Fixer_Upper,
  CASE WHEN p.MarketTotalValue > 0 AND p.AssdImprovementValue > 0 AND (p.AssdImprovementValue / p.MarketTotalValue) < 0.30 AND p.LotSizeAcres >= 0.25 THEN 'Y' ELSE 'N' END AS Value_Add,
  CASE WHEN p.YearBuilt > 0 AND YEAR(CURDATE()) - p.YearBuilt >= 40 THEN 'Y' ELSE 'N' END AS Aging_Property,
  CASE WHEN p.LotSizeAcres >= 1.0 OR (p.LotSizeSqFt >= 21780 AND p.PropertyClassID = 'R') THEN 'Y' ELSE 'N' END AS Large_Lot,
  CASE WHEN p.SumResidentialUnits > 1 OR (p.PropertyClassID = 'R' AND p.SumBuildingsNbr > 1) THEN 'Y' ELSE 'N' END AS Multi_Unit,
  CASE WHEN p.PropertyClassID = 'R' AND p.Zoning REGEXP '(C-|COM|COMM|BUSINESS|RETAIL|OFFICE|INDUSTRIAL|MIX)' THEN 'Y' ELSE 'N' END AS Commercial_Potential,
  CASE WHEN p.PoolCode IS NOT NULL AND p.PoolCode != '0' AND p.PoolCode != '' THEN 'Y' ELSE 'N' END AS Pool_Present,
  CASE WHEN p.flInsideSFHA = 'Y' OR p.flFemaFloodZone REGEXP '[AEV]' THEN 'Y' ELSE 'N' END AS Flood_Zone,
  CASE WHEN p.HOA1Name IS NOT NULL AND p.HOA1Name != '' THEN 'Y' ELSE 'N' END AS HOA_Property,
  CASE WHEN p.StyleCode = 27 OR p.StyleCode = 57 THEN 'Y' ELSE 'N' END AS Historical_Property,
  CASE WHEN p.MobileHomeInd = 'Y' OR p.StyleCode IN (33, 45) THEN 'Y' ELSE 'N' END AS Mobile_Home,
  CASE WHEN p.BasementCode IS NOT NULL AND p.BasementCode NOT IN ('5', '') THEN 'Y' ELSE 'N' END AS Basement_Present,
  CASE WHEN p.Amenities LIKE '%B%' OR p.Amenities LIKE '%L%' OR p.Amenities LIKE '%W%' OR p.SiteInfluenceCode IN ('1', '2', '3', '4', '5', '6', '7', '8', '10', '11', '12') THEN 'Y' ELSE 'N' END AS View_Amenities_Premium,
  CASE WHEN (p.CurrentSaleDocumentType IN ('34','64','N') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('34','64','N') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.CurrentSaleSeller1FullName LIKE '%BANK%' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) THEN 'Y' ELSE 'N' END AS Foreclosure,
  CASE WHEN (p.CurrentSaleDocumentType IN ('77','78','79','81') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)) OR (p.PrevSaleDocumentType IN ('77','78','79','81') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)) THEN 'Y' ELSE 'N' END AS Pre_Foreclosure,
  CASE WHEN (p.CurrentSaleDocumentType IN ('33','19','13') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('33','19','13') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) THEN 'Y' ELSE 'N' END AS Probate,
  CASE WHEN (p.CurrentSaleDocumentType IN ('29','M') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('29','M') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) THEN 'Y' ELSE 'N' END AS Pre_Probate,
  CASE WHEN p.TaxDeliquentYear >= YEAR(CURDATE()) - 1 THEN 'Y' ELSE 'N' END AS Tax_Delinquent,
  CASE WHEN p.VacantFlag IN ('M','B','Y') THEN 'Y' ELSE 'N' END AS Vacant,
  CASE WHEN p.CurrentSaleDocumentType BETWEEN '27' AND '50' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH) THEN 'Y' ELSE 'N' END AS Special_Document,
  CASE WHEN p.vConfidenceScore > 0 AND p.vConfidenceScore < 50 AND p.vValuationDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 'Y' ELSE 'N' END AS Low_Confidence_Value,
  CASE WHEN YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) > 20 THEN 'Y' ELSE 'N' END AS Long_Term_Ownership,
  CASE WHEN (p.WelfareInd = 'Y' OR p.DisabledInd = 'Y' OR p.WidowInd = 'Y' OR p.VeteranInd = 'Y') THEN 'Y' ELSE 'N' END AS Exemptions_Indigency,
  CASE WHEN p.TaxAmt > 0 AND p.TaxYear < YEAR(CURDATE()) - 2 THEN 'Y' ELSE 'N' END AS Potential_Tax_Issues,
  CASE WHEN p.MarketTotalValue > 0 AND p.AssdTotalValue > 0 AND (p.MarketTotalValue / p.AssdTotalValue) < 0.70 THEN 'Y' ELSE 'N' END AS Low_Market_Value,
  CASE WHEN p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 10 YEAR) THEN 'Y' ELSE 'N' END AS No_Recent_Activity

FROM datatree_property p

WHERE p.PropertyClassID = 'R'
  AND p.OwnerNAME1FULL NOT REGEXP '\\b(BANK|ASSOC|MASJID|CHURCH|TEMPLE|MOSQUE|SYNAGOGUE)\\b'
  AND p.OwnerNAME1FULL NOT LIKE '%MORTGAGE%'
  AND p.OwnerNAME1FULL NOT LIKE '%LENDING%'
  AND p.OwnerNAME1FULL NOT LIKE '%FREDDIE MAC%'
  AND p.OwnerNAME1FULL NOT LIKE '%FANNIE MAE%'
  AND p.OwnerNAME1FULL NOT LIKE '%FEDERAL%'
  AND p.OwnerNAME1FULL NOT LIKE '%HUD%'
  
  -- CRITICAL: Add filters dynamically in PHP
  -- Example: AND p.FIPS = :county
  -- Example: AND p.SitusZIP5 = :zip
  -- PHP will build these conditions and add them here
  
ORDER BY p.CurrentSaleRecordingDate DESC
LIMIT 100; 