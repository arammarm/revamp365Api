
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
    p.OwnerNAME1FULL,
    p.OwnerNAME2FULL,
    p.MailingFullStreetAddress,
    p.MailingCity,
    p.MailingState,
    p.MailingZIP5,
    p.BuildingArea,
    p.LotSizeSqFt,
    p.LotSizeAcres,
    p.YearBuilt,
    p.Bedrooms,
    p.BathTotalCalc,
    p.StoriesNbrCode,
    p.StyleCode,
    p.AssdTotalValue,
    p.MarketTotalValue,

    CASE
        WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b')
            AND p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b' THEN 'Corporate Owned'
        WHEN p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' THEN 'Business Owned'
        ELSE 'Individual Owned'
        END AS OwnershipType,

    -- Pre-computed tag flags (return as separate columns, build JSON array in PHP)
    -- This is MUCH faster than building JSON string in SQL
    (p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress) AS tag_absentee_owner,
    (((p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL))) AS tag_high_equity,
    ((p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10))) AS tag_fixer_upper,
    (p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)) AS tag_recent_sale,
    ((p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b')) AS tag_corporate_owner,
    (p.MailingState != p.SitusState) AS tag_out_of_state_owner,
    (((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%')) AS tag_trust_owned
    ,(((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10))) AS tag_elderly_owner,
    (p.OwnerOccupied = 'Y') AS tag_owner_occupied,
    ((p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL)) AS tag_no_mortgage,
    (p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) < 0) AS tag_negative_equity,
    (p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) BETWEEN 0 AND 0.20) AS tag_low_equity,
    (p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND (p.TotalOpenLienAmt / p.MarketTotalValue) >= 0.80) AS tag_high_ltv,
    (p.ConcurrentMtg1LoanType = '32' AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR)) AS tag_reverse_mortgage,
    ((p.ConcurrentMtg1AdjOrFix = 'ADJ' OR p.ConcurrentMtg1LoanType = '3') AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) AS tag_arm,
    (((p.IsListedFlag = 'Y' OR p.IsListedFlag = '1') AND (p.IsListedFlagDate IS NULL OR p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)))) AS tag_on_market,
    (p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH) AND (p.CurrentSaleRecordingDate IS NULL OR p.CurrentSaleRecordingDate < p.IsListedFlagDate)) AS tag_failed_listing,
    (p.CurrentSalesPrice > 0 AND p.MarketTotalValue > 0 AND (p.CurrentSalesPrice / p.MarketTotalValue) < 0.85 AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) AS tag_below_market_sale,
    (p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 12 MONTH)) AS tag_historical_sale,
    (p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 15 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) AS tag_legacy_sale,
    (p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 15 YEAR)) AS tag_ancient_sale,
    (p.BuildingConditionCode IN (4,5) AND p.MarketTotalValue > 0 AND p.AssdLandValue > 0 AND (p.AssdLandValue / p.MarketTotalValue) >= 0.60) AS tag_flip_potential,
    (p.MarketTotalValue > 0 AND p.AssdImprovementValue > 0 AND (p.AssdImprovementValue / p.MarketTotalValue) < 0.30 AND p.LotSizeAcres >= 0.25) AS tag_value_add,
    (p.YearBuilt > 0 AND YEAR(CURDATE()) - p.YearBuilt >= 40) AS tag_aging_property,
    (p.LotSizeAcres >= 1.0 OR (p.LotSizeSqFt >= 21780 AND p.PropertyClassID = 'R')) AS tag_large_lot,
    (p.SumResidentialUnits > 1 OR (p.PropertyClassID = 'R' AND p.SumBuildingsNbr > 1)) AS tag_multi_unit,
    (p.PropertyClassID = 'R' AND p.Zoning REGEXP '(C-|COM|COMM|BUSINESS|RETAIL|OFFICE|INDUSTRIAL|MIX)') AS tag_commercial_potential,
    (p.PoolCode IS NOT NULL AND p.PoolCode != '0' AND p.PoolCode != '') AS tag_pool_present,
    (p.flInsideSFHA = 'Y' OR p.flFemaFloodZone REGEXP '[AEV]') AS tag_flood_zone,
    (p.HOA1Name IS NOT NULL AND p.HOA1Name != '') AS tag_hoa_property,
    (p.StyleCode = 27 OR p.StyleCode = 57) AS tag_historical_property,
    (p.MobileHomeInd = 'Y' OR p.StyleCode IN (33, 45)) AS tag_mobile_home,
    (p.BasementCode IS NOT NULL AND p.BasementCode NOT IN ('5', '')) AS tag_basement_present,
    (p.Amenities LIKE '%B%' OR p.Amenities LIKE '%L%' OR p.Amenities LIKE '%W%' OR p.SiteInfluenceCode IN ('1', '2', '3', '4', '5', '6', '7', '8', '10', '11', '12')) AS tag_view_amenities_premium,
    (((p.CurrentSaleDocumentType IN ('34','64','N') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('34','64','N') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.CurrentSaleSeller1FullName LIKE '%BANK%' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))) AS tag_foreclosure,
    (((p.CurrentSaleDocumentType IN ('77','78','79','81') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)) OR (p.PrevSaleDocumentType IN ('77','78','79','81') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)))) AS tag_pre_foreclosure,
    (((p.CurrentSaleDocumentType IN ('33','19','13') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('33','19','13') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))) AS tag_probate,
    (((p.CurrentSaleDocumentType IN ('29','M') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('29','M') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))) AS tag_pre_probate,
    (p.TaxDeliquentYear >= YEAR(CURDATE()) - 1) AS tag_tax_delinquent,
    (p.VacantFlag IN ('M','B','Y')) AS tag_vacant,
    (p.CurrentSaleDocumentType BETWEEN '27' AND '50' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) AS tag_special_document,
    (p.vConfidenceScore > 0 AND p.vConfidenceScore < 50 AND p.vValuationDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)) AS tag_low_confidence_value,
    (YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) > 20) AS tag_long_term_ownership,
    ((p.WelfareInd = 'Y' OR p.DisabledInd = 'Y' OR p.WidowInd = 'Y' OR p.VeteranInd = 'Y')) AS tag_exemptions_indigency,
    (p.TaxAmt > 0 AND p.TaxYear < YEAR(CURDATE()) - 2) AS tag_potential_tax_issues,
    (p.MarketTotalValue > 0 AND p.AssdTotalValue > 0 AND (p.MarketTotalValue / p.AssdTotalValue) < 0.70) AS tag_low_market_value,
    (p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 10 YEAR)) AS tag_no_recent_activity

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
-- AND p.FIPS = :county
-- Example: AND p.SitusZIP5 = :zip

ORDER BY p.CurrentSaleRecordingDate DESC
LIMIT 100;