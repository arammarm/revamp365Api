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
  -- Built manually using CONCAT for MySQL 5.7 compatibility
  CONCAT(
    '[',
    -- Tag 1: Absentee Owner (priority 1)
    CASE WHEN (p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress) THEN 
      CONCAT('"Absentee Owner"',
        -- Tag 2: High Equity (priority 2)
        CASE WHEN ((p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL)) THEN 
          CONCAT(',"High Equity"',
            -- Tag 3: Fixer Upper (priority 3)
            CASE WHEN (p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10)) THEN 
              CONCAT(',"Fixer Upper"',
                -- Tag 4: Recent Sale (priority 4)
                CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN ',"Recent Sale"' ELSE '' END
              ) ELSE 
              -- Tag 3: Recent Sale (if Fixer Upper not applicable)
              CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
                CONCAT(',"Recent Sale"',
                  -- Tag 4: Corporate Owner
                  CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN ',"Corporate Owner"' ELSE '' END
                ) ELSE 
                -- Tag 3: Corporate Owner (if Recent Sale not applicable)
                CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                  CONCAT(',"Corporate Owner"',
                    -- Tag 4: Out Of State Owner
                    CASE WHEN p.MailingState != p.SitusState THEN ',"Out Of State Owner"' ELSE '' END
                  ) ELSE 
                  -- Tag 3: Out Of State Owner
                  CASE WHEN p.MailingState != p.SitusState THEN 
                    CONCAT(',"Out Of State Owner"',
                      -- Tag 4: Trust Owned
                      CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                    ) ELSE '' END
                END
              END
            END
          ) ELSE 
          -- Tag 2: Fixer Upper (if High Equity not applicable)
          CASE WHEN (p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10)) THEN 
            CONCAT(',"Fixer Upper"',
              CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
                CONCAT(',"Recent Sale"',
                  CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN ',"Corporate Owner"' ELSE '' END
                ) ELSE 
                CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                  CONCAT(',"Corporate Owner"',
                    CASE WHEN p.MailingState != p.SitusState THEN ',"Out Of State Owner"' ELSE '' END
                  ) ELSE 
                  CASE WHEN p.MailingState != p.SitusState THEN 
                    CONCAT(',"Out Of State Owner"',
                      CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                    ) ELSE '' END
                END
              END
            ) ELSE 
            -- Tag 2: Recent Sale (if Fixer Upper not applicable)
            CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
              CONCAT(',"Recent Sale"',
                CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                  CONCAT(',"Corporate Owner"',
                    CASE WHEN p.MailingState != p.SitusState THEN ',"Out Of State Owner"' ELSE '' END
                  ) ELSE 
                  CASE WHEN p.MailingState != p.SitusState THEN 
                    CONCAT(',"Out Of State Owner"',
                      CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                    ) ELSE '' END
                END
              ) ELSE 
              -- Tag 2: Corporate Owner
              CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                CONCAT(',"Corporate Owner"',
                  CASE WHEN p.MailingState != p.SitusState THEN 
                    CONCAT(',"Out Of State Owner"',
                      CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                    ) ELSE 
                    CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                  END
                ) ELSE 
                -- Tag 2: Out Of State Owner
                CASE WHEN p.MailingState != p.SitusState THEN 
                  CONCAT(',"Out Of State Owner"',
                    CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN 
                      CONCAT(',"Trust Owned"',
                        CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                      ) ELSE 
                      CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                    END
                  ) ELSE '' END
              END
            END
          END
        ) ELSE 
        -- Tag 1: High Equity (if Absentee Owner not applicable)
        CASE WHEN ((p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL)) THEN 
          CONCAT('"High Equity"',
            CASE WHEN (p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10)) THEN 
              CONCAT(',"Fixer Upper"',
                CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
                  CONCAT(',"Recent Sale"',
                    CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN ',"Corporate Owner"' ELSE '' END
                  ) ELSE 
                  CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                    CONCAT(',"Corporate Owner"',
                      CASE WHEN p.MailingState != p.SitusState THEN ',"Out Of State Owner"' ELSE '' END
                    ) ELSE 
                    CASE WHEN p.MailingState != p.SitusState THEN 
                      CONCAT(',"Out Of State Owner"',
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                      ) ELSE '' END
                  END
                END
              ) ELSE 
              CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
                CONCAT(',"Recent Sale"',
                  CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                    CONCAT(',"Corporate Owner"',
                      CASE WHEN p.MailingState != p.SitusState THEN ',"Out Of State Owner"' ELSE '' END
                    ) ELSE 
                    CASE WHEN p.MailingState != p.SitusState THEN 
                      CONCAT(',"Out Of State Owner"',
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                      ) ELSE '' END
                  END
                ) ELSE 
                CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                  CONCAT(',"Corporate Owner"',
                    CASE WHEN p.MailingState != p.SitusState THEN 
                      CONCAT(',"Out Of State Owner"',
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                      ) ELSE 
                      CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                    END
                  ) ELSE 
                  CASE WHEN p.MailingState != p.SitusState THEN 
                    CONCAT(',"Out Of State Owner"',
                      CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN 
                        CONCAT(',"Trust Owned"',
                          CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                        ) ELSE 
                        CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                      END
                    ) ELSE '' END
                END
              END
            ) ELSE 
            -- Tag 1: Fixer Upper (if High Equity not applicable)
            CASE WHEN (p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10)) THEN 
              CONCAT('"Fixer Upper"',
                CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
                  CONCAT(',"Recent Sale"',
                    CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                      CONCAT(',"Corporate Owner"',
                        CASE WHEN p.MailingState != p.SitusState THEN ',"Out Of State Owner"' ELSE '' END
                      ) ELSE 
                      CASE WHEN p.MailingState != p.SitusState THEN 
                        CONCAT(',"Out Of State Owner"',
                          CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                        ) ELSE '' END
                    END
                  ) ELSE 
                  CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                    CONCAT(',"Corporate Owner"',
                      CASE WHEN p.MailingState != p.SitusState THEN 
                        CONCAT(',"Out Of State Owner"',
                          CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                        ) ELSE 
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                      END
                    ) ELSE 
                    CASE WHEN p.MailingState != p.SitusState THEN 
                      CONCAT(',"Out Of State Owner"',
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN 
                          CONCAT(',"Trust Owned"',
                            CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                          ) ELSE 
                          CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                        END
                      ) ELSE '' END
                  END
                END
              ) ELSE 
              -- Tag 1: Recent Sale (if Fixer Upper not applicable)
              CASE WHEN p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 
                CONCAT('"Recent Sale"',
                  CASE WHEN (p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\b(LLC|INC|CORP|LP|LTD)\\b') THEN 
                    CONCAT(',"Corporate Owner"',
                      CASE WHEN p.MailingState != p.SitusState THEN 
                        CONCAT(',"Out Of State Owner"',
                          CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                        ) ELSE 
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN ',"Trust Owned"' ELSE '' END
                      END
                    ) ELSE 
                    CASE WHEN p.MailingState != p.SitusState THEN 
                      CONCAT(',"Out Of State Owner"',
                        CASE WHEN ((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%') THEN 
                          CONCAT(',"Trust Owned"',
                            CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                          ) ELSE 
                          CASE WHEN ((p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)) THEN ',"Elderly Owner"' ELSE '' END
                        END
                      ) ELSE '' END
                  END
                ) ELSE '' END
            END
          END
        ) ELSE '' END
    ) ELSE '' END,
    ']'
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