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
  -- p.Owner1LastName, -- Commented: Not needed in response
  -- p.Owner1FirstName, -- Commented: Not needed in response
  -- p.Owner2LastName, -- Commented: Not needed in response
  -- p.Owner2FirstName, -- Commented: Not needed in response
  p.OwnerNAME1FULL,
  p.OwnerNAME2FULL,
  -- p.Owner1CorpInd, -- Commented: Used in WHERE conditions, not needed in response
  -- p.Owner2CorpInd, -- Commented: Used in WHERE conditions, not needed in response
  -- p.OwnerOccupied, -- Commented: Used in WHERE conditions, not needed in response
  p.MailingFullStreetAddress,
  p.MailingCity,
  p.MailingState,
  p.MailingZIP5,
  -- p.AssdTotalValue, -- Commented: Used in WHERE conditions, not needed in response
  -- p.AssdLandValue, -- Commented: Used in WHERE conditions, not needed in response
  -- p.AssdImprovementValue, -- Commented: Used in WHERE conditions, not needed in response
  -- p.MarketTotalValue, -- Commented: Used in WHERE conditions, not needed in response
  -- p.TaxAmt, -- Commented: Used in WHERE conditions, not needed in response
  p.BuildingArea,
  p.LotSizeSqFt,
  p.LotSizeAcres,
  p.YearBuilt,
  p.Bedrooms,
  p.BathTotalCalc,
  p.StoriesNbrCode,
  -- p.CurrentSaleRecordingDate, -- Commented: Used in WHERE conditions and ORDER BY, not needed in response
  -- p.CurrentSalesPrice, -- Commented: Used in WHERE conditions, not needed in response
  -- p.ConcurrentMtg1LoanAmt, -- Commented: Not needed in response
  -- p.ConcurrentMtg1Term, -- Commented: Not needed in response
  -- p.ConcurrentMtg1InterestRate, -- Commented: Not needed in response
  -- p.ConcurrentMtg1Lender, -- Commented: Not needed in response
  -- p.ConcurrentMtg1AdjOrFix, -- Commented: Used in WHERE conditions, not needed in response
  -- p.ConcurrentMtg1LoanType, -- Commented: Used in WHERE conditions, not needed in response
  -- p.ConcurrentMtg1RecordingDate, -- Commented: Used in WHERE conditions, not needed in response
  -- p.ConcurrentMtg2LoanAmt, -- Commented: Not needed in response
  -- p.ConcurrentMtg2Lender, -- Commented: Not needed in response
  -- p.TotalOpenLienNbr, -- Commented: Not needed in response
  -- p.TotalOpenLienAmt, -- Commented: Used in WHERE conditions, not needed in response
  -- p.IsListedFlag, -- Commented: Used in WHERE conditions, not needed in response
  -- p.IsListedFlagDate, -- Commented: Used in WHERE conditions, not needed in response
  -- p.IsListedPriceRange, -- Commented: Not needed in response
  p.StyleCode,
  
  -- Calculated Fields
  -- CASE 
  --   WHEN p.CurrentSaleRecordingDate IS NOT NULL THEN 
  --     YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate)
  --   ELSE NULL 
  -- END AS YearsOfOwnership, -- Commented: Not needed in response
  
  -- CASE 
  --   WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 THEN
  --     ROUND(((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) * 100, 2)
  --   WHEN p.MarketTotalValue > 0 AND (p.TotalOpenLienAmt IS NULL OR p.TotalOpenLienAmt = 0) THEN
  --     100.00
  --   ELSE NULL
  -- END AS EquityPercent, -- Commented: Not needed in response
  
  -- CASE 
  --   WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 THEN
  --     p.MarketTotalValue - p.TotalOpenLienAmt
  --   WHEN p.MarketTotalValue > 0 AND (p.TotalOpenLienAmt IS NULL OR p.TotalOpenLienAmt = 0) THEN
  --     p.MarketTotalValue
  --   ELSE NULL
  -- END AS EquityValue, -- Commented: Not needed in response
  
  -- CASE 
  --   WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 THEN
  --     ROUND((p.TotalOpenLienAmt / p.MarketTotalValue) * 100, 2)
  --   ELSE 0.00
  -- END AS LTV_Percent, -- Commented: Not needed in response
  
  -- Computed Fields: OwnershipType and Tags (replaces 47 individual Y/N tag fields)
  CASE 
    WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') 
         AND p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b' THEN 'Corporate Owned'
    WHEN p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' THEN 'Business Owned'
    ELSE 'Individual Owned'
  END AS OwnershipType,
  
  -- Tags: JSON array string (max 4 items, priority order: Absentee_Owner, High_Equity, Fixer_Upper, Recent_Sale, then others)
  -- Returns JSON array with all tags in priority order (may include NULLs). PHP will filter NULLs and limit to 4 items.
  JSON_ARRAY(
    CASE WHEN (p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress) THEN 'Absentee Owner' ELSE NULL END,
    CASE WHEN ((p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL)) THEN 'High Equity' ELSE NULL END,
    CASE WHEN (p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10)) THEN 'Fixer Upper' ELSE NULL END,
    CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 'Recent Sale' ELSE NULL END,
    CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 'Corporate Owner' ELSE NULL END,
    CASE WHEN p.MailingState != p.SitusState THEN 'Out Of State Owner' ELSE NULL END,
    CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN 'Trust Owned' ELSE NULL END,
    CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN 'Elderly Owner' ELSE NULL END,
    CASE WHEN p.OwnerOccupied = 'Y' THEN 'Owner Occupied' ELSE NULL END,
    CASE WHEN (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL) THEN 'No Mortgage' ELSE NULL END,
    CASE WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) < 0 THEN 'Negative Equity' ELSE NULL END,
    CASE WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) BETWEEN 0 AND 0.20 THEN 'Low Equity' ELSE NULL END,
    CASE WHEN p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND (p.TotalOpenLienAmt / p.MarketTotalValue) >= 0.80 THEN 'High LTV' ELSE NULL END,
    CASE WHEN p.ConcurrentMtg1LoanType = '32' AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR) THEN 'Reverse Mortgage' ELSE NULL END,
    CASE WHEN (p.ConcurrentMtg1AdjOrFix = 'ADJ' OR p.ConcurrentMtg1LoanType = '3') AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) THEN 'ARM' ELSE NULL END,
    CASE WHEN ((p.IsListedFlag = 'Y' OR p.IsListedFlag = '1') AND (p.IsListedFlagDate IS NULL OR p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH))) THEN 'On Market' ELSE NULL END,
    CASE WHEN p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH) AND (p.CurrentSaleRecordingDate IS NULL OR p.CurrentSaleRecordingDate < p.IsListedFlagDate) THEN 'Failed Listing' ELSE NULL END,
    CASE WHEN p.CurrentSalesPrice > 0 AND p.MarketTotalValue > 0 AND (p.CurrentSalesPrice / p.MarketTotalValue) < 0.85 AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH) THEN 'Below Market Sale' ELSE NULL END,
    CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 'Historical Sale' ELSE NULL END,
    CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 15 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 5 YEAR) THEN 'Legacy Sale' ELSE NULL END,
    CASE WHEN p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 15 YEAR) THEN 'Ancient Sale' ELSE NULL END,
    CASE WHEN p.BuildingConditionCode IN (4,5) AND p.MarketTotalValue > 0 AND p.AssdLandValue > 0 AND (p.AssdLandValue / p.MarketTotalValue) >= 0.60 THEN 'Flip Potential' ELSE NULL END,
    CASE WHEN p.MarketTotalValue > 0 AND p.AssdImprovementValue > 0 AND (p.AssdImprovementValue / p.MarketTotalValue) < 0.30 AND p.LotSizeAcres >= 0.25 THEN 'Value Add' ELSE NULL END,
    CASE WHEN p.YearBuilt > 0 AND YEAR(CURDATE()) - p.YearBuilt >= 40 THEN 'Aging Property' ELSE NULL END,
    CASE WHEN p.LotSizeAcres >= 1.0 OR (p.LotSizeSqFt >= 21780 AND p.PropertyClassID = 'R') THEN 'Large Lot' ELSE NULL END,
    CASE WHEN p.SumResidentialUnits > 1 OR (p.PropertyClassID = 'R' AND p.SumBuildingsNbr > 1) THEN 'Multi Unit' ELSE NULL END,
    CASE WHEN p.PropertyClassID = 'R' AND p.Zoning REGEXP '(C-|COM|COMM|BUSINESS|RETAIL|OFFICE|INDUSTRIAL|MIX)' THEN 'Commercial Potential' ELSE NULL END,
    CASE WHEN p.PoolCode IS NOT NULL AND p.PoolCode != '0' AND p.PoolCode != '' THEN 'Pool Present' ELSE NULL END,
    CASE WHEN p.flInsideSFHA = 'Y' OR p.flFemaFloodZone REGEXP '[AEV]' THEN 'Flood Zone' ELSE NULL END,
    CASE WHEN p.HOA1Name IS NOT NULL AND p.HOA1Name != '' THEN 'HOA Property' ELSE NULL END,
    CASE WHEN p.StyleCode = 27 OR p.StyleCode = 57 THEN 'Historical Property' ELSE NULL END,
    CASE WHEN p.MobileHomeInd = 'Y' OR p.StyleCode IN (33, 45) THEN 'Mobile Home' ELSE NULL END,
    CASE WHEN p.BasementCode IS NOT NULL AND p.BasementCode NOT IN ('5', '') THEN 'Basement Present' ELSE NULL END,
    CASE WHEN p.Amenities LIKE '%B%' OR p.Amenities LIKE '%L%' OR p.Amenities LIKE '%W%' OR p.SiteInfluenceCode IN ('1', '2', '3', '4', '5', '6', '7', '8', '10', '11', '12') THEN 'View Amenities Premium' ELSE NULL END,
    CASE WHEN ((p.CurrentSaleDocumentType IN ('34','64','N') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('34','64','N') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.CurrentSaleSeller1FullName LIKE '%BANK%' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH))) THEN 'Foreclosure' ELSE NULL END,
    CASE WHEN ((p.CurrentSaleDocumentType IN ('77','78','79','81') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)) OR (p.PrevSaleDocumentType IN ('77','78','79','81') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH))) THEN 'Pre Foreclosure' ELSE NULL END,
    CASE WHEN ((p.CurrentSaleDocumentType IN ('33','19','13') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('33','19','13') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH))) THEN 'Probate' ELSE NULL END,
    CASE WHEN ((p.CurrentSaleDocumentType IN ('29','M') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('29','M') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH))) THEN 'Pre Probate' ELSE NULL END,
    CASE WHEN p.TaxDeliquentYear >= YEAR(CURDATE()) - 1 THEN 'Tax Delinquent' ELSE NULL END,
    CASE WHEN p.VacantFlag IN ('M','B','Y') THEN 'Vacant' ELSE NULL END,
    CASE WHEN p.CurrentSaleDocumentType BETWEEN '27' AND '50' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH) THEN 'Special Document' ELSE NULL END,
    CASE WHEN p.vConfidenceScore > 0 AND p.vConfidenceScore < 50 AND p.vValuationDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 'Low Confidence Value' ELSE NULL END,
    CASE WHEN YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) > 20 THEN 'Long Term Ownership' ELSE NULL END,
    CASE WHEN (p.WelfareInd = 'Y' OR p.DisabledInd = 'Y' OR p.WidowInd = 'Y' OR p.VeteranInd = 'Y') THEN 'Exemptions Indigency' ELSE NULL END,
    CASE WHEN p.TaxAmt > 0 AND p.TaxYear < YEAR(CURDATE()) - 2 THEN 'Potential Tax Issues' ELSE NULL END,
    CASE WHEN p.MarketTotalValue > 0 AND p.AssdTotalValue > 0 AND (p.MarketTotalValue / p.AssdTotalValue) < 0.70 THEN 'Low Market Value' ELSE NULL END,
    CASE WHEN p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 10 YEAR) THEN 'No Recent Activity' ELSE NULL END
  ) AS Tags

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