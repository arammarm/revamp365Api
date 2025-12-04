<?php
/**
 * Streaming version of unlisted_search.php
 * 
 * This version streams results incrementally instead of loading all results into memory.
 * Key differences:
 * - Uses fetch() instead of fetchAll() to process rows one at a time
 * - Outputs JSON incrementally as rows are processed
 * - Flushes output every 100 rows to keep memory usage low
 * - Increased max limit to 1,000,000 for unlimited results
 * - Response time is much faster for large result sets (500k+ records)
 * 
 * The response format is the same as the original API, but data is streamed
 * so the client can start processing results immediately.
 */

include_once "mysql_conn.php";
include_once "../includes/APIHelperV2.php";
include_once "../includes/DatatreeAPIHelper.php";
include_once "../includes/DatatreeAPIHelperExtended.php"; 

APIHelper::Authentication(true); 

global $dsn, $username, $password;

// Include the InvestorQueryBuilder class from the original file
class InvestorQueryBuilder
{

    private $baseQuery;
    private $whereConditions = [];
    private $params = [];

    private $defaultLimit = 150000;
    private $maxLimit = 150000; // Increased for streaming (1 million)

    public function __construct()
    {
        // Load the base query
        $this->baseQuery = file_get_contents(__DIR__ . '/unlisted-query/query.sql');
    }

    /**
     * Build the complete query with all filters
     */
    public function buildQuery($filters)
    {
        $this->whereConditions = [];
        $this->params = [];

        // PROPERTY IDS FILTER (if provided, this is the primary filter)
        $this->addPropertyIdsFilter($filters);

        // GEOGRAPHIC FILTERS
        $this->addCountyFilter($filters);
        $this->addCityFilter($filters);
        $this->addZipFilter($filters);
        $this->addSchoolDistrictFilter($filters);

        // OWNER TYPE FILTERS
        $this->addOwnerTypeFilters($filters);

        // ABSENTEE & LOCATION FILTERS
        $this->addAbsenteeFilter($filters);
        $this->addLocationFilters($filters);

        // OWNERSHIP METRICS
        $this->addYearsOwnedFilter($filters);

        // SALE DATE FILTERS
        $this->addSaleDateFilter($filters);

        // OWNER NAME FILTERS
        $this->addOwnerFirstNameFilter($filters);
        $this->addOwnerLastNameFilter($filters);

        // SALE PRICE FILTERS
        $this->addSalePriceFilter($filters);

        // PROPERTY CHARACTERISTICS
        $this->addBedroomFilter($filters);
        $this->addBathroomFilter($filters);
        $this->addBuildingSqftFilter($filters);
        $this->addLotSizeFilter($filters);
        $this->addYearBuiltFilter($filters);
        $this->addStoriesFilter($filters);

        // VALUE FILTERS
        $this->addMarketValueFilter($filters);
        $this->addAssessedValueFilters($filters);

        // EQUITY FILTERS
        $this->addEquityFilters($filters);

        // LOAN FILTERS
        $this->addLoanFilters($filters);

        // MOTIVATION TAG FILTERS
        $this->addMotivationTagFilters($filters);

        // NEW FILTERS
        $this->addStructureTypeFilter($filters);
        $this->addOwnerOccupiedFilter($filters);
        $this->addTagBasedFilters($filters);

        // Build final query
        return $this->getFinalQuery($filters);
    }

    /**
     * Get parameters for PDO binding
     */
    public function getParams()
    {
        return $this->params;
    }

    /**
     * Build Tags array from row data (max 4 items, priority order)
     * @param array $row
     * @return array
     */
    public static function buildTagsFromRow($row): array
    {
        $tagMap = [
            'tag_absentee_owner' => 'Absentee Owner',
            'tag_high_equity' => 'High Equity',
            'tag_fixer_upper' => 'Fixer Upper',
            'tag_recent_sale' => 'Recent Sale',
            'tag_corporate_owner' => 'Corporate Owner',
            'tag_out_of_state_owner' => 'Out Of State Owner',
            'tag_trust_owned' => 'Trust Owned',
            'tag_elderly_owner' => 'Elderly Owner',
            'tag_owner_occupied' => 'Owner Occupied',
            'tag_no_mortgage' => 'No Mortgage',
            'tag_negative_equity' => 'Negative Equity',
            'tag_low_equity' => 'Low Equity',
            'tag_high_ltv' => 'High LTV',
            'tag_reverse_mortgage' => 'Reverse Mortgage',
            'tag_arm' => 'ARM',
            'tag_on_market' => 'On Market',
            'tag_failed_listing' => 'Failed Listing',
            'tag_below_market_sale' => 'Below Market Sale',
            'tag_historical_sale' => 'Historical Sale',
            'tag_legacy_sale' => 'Legacy Sale',
            'tag_ancient_sale' => 'Ancient Sale',
            'tag_flip_potential' => 'Flip Potential',
            'tag_value_add' => 'Value Add',
            'tag_aging_property' => 'Aging Property',
            'tag_large_lot' => 'Large Lot',
            'tag_multi_unit' => 'Multi Unit',
            'tag_commercial_potential' => 'Commercial Potential',
            'tag_pool_present' => 'Pool Present',
            'tag_flood_zone' => 'Flood Zone',
            'tag_hoa_property' => 'HOA Property',
            'tag_historical_property' => 'Historical Property',
            'tag_mobile_home' => 'Mobile Home',
            'tag_basement_present' => 'Basement Present',
            'tag_view_amenities_premium' => 'View Amenities Premium',
            'tag_foreclosure' => 'Foreclosure',
            'tag_pre_foreclosure' => 'Pre Foreclosure',
            'tag_probate' => 'Probate',
            'tag_pre_probate' => 'Pre Probate',
            'tag_tax_delinquent' => 'Tax Delinquent',
            'tag_vacant' => 'Vacant',
            'tag_special_document' => 'Special Document',
            'tag_low_confidence_value' => 'Low Confidence Value',
            'tag_long_term_ownership' => 'Long Term Ownership',
            'tag_exemptions_indigency' => 'Exemptions Indigency',
            'tag_potential_tax_issues' => 'Potential Tax Issues',
            'tag_low_market_value' => 'Low Market Value',
            'tag_no_recent_activity' => 'No Recent Activity',
        ];
        
        $tags = [];
        foreach ($tagMap as $column => $label) {
            // Check if the tag field exists and is truthy (1, true, '1', etc.)
            // MySQL boolean expressions return 0 or 1 as integers
            if (isset($row[$column]) && (bool)$row[$column] && count($tags) < 4) {
                $tags[] = $label;
            }
        }
        
        return $tags;
    }

    // ===================================================================
    // FILTER METHODS (same as original)
    // ===================================================================

    /**
     * Check if a value is valid for filtering (not null, empty, or 0)
     * @param mixed $value
     * @return bool
     */
    private function isValidFilterValue($value)
    {
        // If value is not set, it's invalid
        if (!isset($value)) {
            return false;
        }

        // If null or empty string, invalid
        if ($value === null || $value === '') {
            return false;
        }

        // If string '0' or integer 0 or float 0.0, invalid
        if ($value === '0' || $value === 0 || $value === 0.0) {
            return false;
        }

        // If numeric and equals zero, invalid
        if (is_numeric($value) && (float) $value == 0) {
            return false;
        }

        // All other cases are valid
        return true;
    }

    private function addPropertyIdsFilter($filters)
    {
        if (!empty($filters['property_ids'])) {
            // Handle both array and single string value
            $propertyIds = [];
            if (is_array($filters['property_ids'])) {
                $propertyIds = $filters['property_ids'];
            } else {
                // Single value as string
                $propertyIds = [$filters['property_ids']];
            }

            // Filter out empty values and trim
            $propertyIds = array_filter(array_map('trim', $propertyIds), function ($id) {
                return !empty($id);
            });

            if (!empty($propertyIds)) {
                // Create placeholders for IN clause
                $placeholders = [];
                foreach ($propertyIds as $index => $id) {
                    $placeholder = 'property_id_' . $index;
                    $placeholders[] = ':' . $placeholder;
                    $this->params[$placeholder] = $id;
                }

                $this->whereConditions[] = "p.PropertyID IN (" . implode(', ', $placeholders) . ")";
            }
        }
    }

    private function addCountyFilter($filters)
    {
        if (!empty($filters['county'])) {
            $this->whereConditions[] = "p.FIPS = :county";
            $this->params['county'] = $filters['county'];
        }
    }

    private function addCityFilter($filters)
    {
        if (!empty($filters['city'])) {
            $this->whereConditions[] = "p.SitusCity = :city";
            $this->params['city'] = $filters['city'];
        }
    }

    private function addZipFilter($filters)
    {
        if (!empty($filters['zip'])) {
            $this->whereConditions[] = "p.SitusZIP5 = :zip";
            $this->params['zip'] = $filters['zip'];
        }
    }

    private function addSchoolDistrictFilter($filters)
    {
        if (!empty($filters['school_district'])) {
            $this->whereConditions[] = "p.SchoolDistrictName LIKE :school_district";
            $this->params['school_district'] = '%' . $filters['school_district'] . '%';
        }
    }

    private function addOwnerTypeFilters($filters)
    {
        if (!empty($filters['owner_type']) && is_array($filters['owner_type'])) {
            $ownerTypeConditions = [];

            if (in_array('individual', $filters['owner_type'])) {
                $ownerTypeConditions[] = "(p.Owner1CorpInd != 'T' AND p.Owner2CorpInd != 'T' AND p.OwnerNAME1FULL NOT REGEXP '\\\\b(LLC|INC|CORP|LP|LTD)\\\\b')";
            }

            if (in_array('business', $filters['owner_type'])) {
                $ownerTypeConditions[] = "(p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\\\b(LLC|INC|CORP|LP|LTD)\\\\b')";
            }

            if (in_array('bank', $filters['owner_type'])) {
                $ownerTypeConditions[] = "(p.OwnerNAME1FULL LIKE '%BANK%' OR p.OwnerNAME1FULL LIKE '%TRUST%')";
            }

            if (!empty($ownerTypeConditions)) {
                $this->whereConditions[] = "(" . implode(" OR ", $ownerTypeConditions) . ")";
            }
        }
    }

    private function addAbsenteeFilter($filters)
    {
        if (isset($filters['absentee_owner']) && $filters['absentee_owner'] !== '') {
            if ($filters['absentee_owner'] === 'Y') {
                $this->whereConditions[] = "(p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress)";
            } else {
                $this->whereConditions[] = "(p.OwnerOccupied = 'Y' AND p.SitusFullStreetAddress = p.MailingFullStreetAddress)";
            }
        }
    }

    private function addLocationFilters($filters)
    {
        if (!empty($filters['in_state_owner'])) {
            $this->whereConditions[] = "p.MailingState = p.SitusState";
        }

        if (!empty($filters['out_of_state_owner'])) {
            $this->whereConditions[] = "p.MailingState != p.SitusState";
        }
    }

    private function addYearsOwnedFilter($filters)
    {
        if ($this->isValidFilterValue($filters['years_owned_min'] ?? null)) {
            $this->whereConditions[] = "YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= :years_owned_min";
            $this->params['years_owned_min'] = $filters['years_owned_min'];
        }

        if ($this->isValidFilterValue($filters['years_owned_max'] ?? null)) {
            $this->whereConditions[] = "YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) <= :years_owned_max";
            $this->params['years_owned_max'] = $filters['years_owned_max'];
        }
    }

    private function addSaleDateFilter($filters)
    {
        // Validate and add last_sale_date_min
        if (!empty($filters['last_sale_date_min'])) {
            $dateMin = $filters['last_sale_date_min'];
            // Validate date format (YYYY-MM-DD)
            if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $dateMin) && strtotime($dateMin) !== false) {
                $this->whereConditions[] = "p.CurrentSaleRecordingDate >= :last_sale_date_min";
                $this->params['last_sale_date_min'] = $dateMin;
            }
        }

        // Validate and add last_sale_date_max
        if (!empty($filters['last_sale_date_max'])) {
            $dateMax = $filters['last_sale_date_max'];
            // Validate date format (YYYY-MM-DD)
            if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $dateMax) && strtotime($dateMax) !== false) {
                $this->whereConditions[] = "p.CurrentSaleRecordingDate <= :last_sale_date_max";
                $this->params['last_sale_date_max'] = $dateMax;
            }
        }

        // Validate that min is not greater than max if both are provided
        if (!empty($filters['last_sale_date_min']) && !empty($filters['last_sale_date_max'])) {
            $dateMin = $filters['last_sale_date_min'];
            $dateMax = $filters['last_sale_date_max'];
            if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $dateMin) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $dateMax)) {
                if (strtotime($dateMin) > strtotime($dateMax)) {
                    // If min > max, swap them or remove the invalid condition
                    // For now, we'll just log/ignore - in production you might want to throw an error
                    // Uncomment the line below if you want to throw an error:
                    // throw new InvalidArgumentException('last_sale_date_min cannot be greater than last_sale_date_max');
                }
            }
        }
    }

    private function addOwnerFirstNameFilter($filters)
    {
        if (!empty($filters['owner_first_name'])) {
            $this->whereConditions[] = "LOWER(p.OwnerNAME1FULL) LIKE LOWER(:owner_first_name)";
            $this->params['owner_first_name'] = '%' . trim($filters['owner_first_name']) . '%';
        }
    }

    private function addOwnerLastNameFilter($filters)
    {
        if (!empty($filters['owner_last_name'])) {
            $this->whereConditions[] = "LOWER(p.OwnerNAME1FULL) LIKE LOWER(:owner_last_name)";
            $this->params['owner_last_name'] = '%' . trim($filters['owner_last_name']) . '%';
        }
    }

    private function addSalePriceFilter($filters)
    {
        if ($this->isValidFilterValue($filters['sale_price_min'] ?? null)) {
            $this->whereConditions[] = "p.CurrentSalesPrice >= :sale_price_min";
            $this->params['sale_price_min'] = $filters['sale_price_min'];
        }

        if ($this->isValidFilterValue($filters['sale_price_max'] ?? null)) {
            $this->whereConditions[] = "p.CurrentSalesPrice <= :sale_price_max";
            $this->params['sale_price_max'] = $filters['sale_price_max'];
        }
    }

    private function addBedroomFilter($filters)
    {
        if ($this->isValidFilterValue($filters['bedrooms_min'] ?? null)) {
            $this->whereConditions[] = "p.Bedrooms >= :bedrooms_min";
            $this->params['bedrooms_min'] = $filters['bedrooms_min'];
        }

        if ($this->isValidFilterValue($filters['bedrooms_max'] ?? null)) {
            $this->whereConditions[] = "p.Bedrooms <= :bedrooms_max";
            $this->params['bedrooms_max'] = $filters['bedrooms_max'];
        }
    }

    private function addBathroomFilter($filters)
    {
        if ($this->isValidFilterValue($filters['bathrooms_min'] ?? null)) {
            $this->whereConditions[] = "p.BathTotalCalc >= :bathrooms_min";
            $this->params['bathrooms_min'] = $filters['bathrooms_min'];
        }

        if ($this->isValidFilterValue($filters['bathrooms_max'] ?? null)) {
            $this->whereConditions[] = "p.BathTotalCalc <= :bathrooms_max";
            $this->params['bathrooms_max'] = $filters['bathrooms_max'];
        }
    }

    private function addBuildingSqftFilter($filters)
    {
        if ($this->isValidFilterValue($filters['building_area_min'] ?? null)) {
            $this->whereConditions[] = "p.BuildingArea >= :building_area_min";
            $this->params['building_area_min'] = $filters['building_area_min'];
        }

        if ($this->isValidFilterValue($filters['building_area_max'] ?? null)) {
            $this->whereConditions[] = "p.BuildingArea <= :building_area_max";
            $this->params['building_area_max'] = $filters['building_area_max'];
        }
    }

    private function addLotSizeFilter($filters)
    {
        if ($this->isValidFilterValue($filters['lot_size_min'] ?? null)) {
            $this->whereConditions[] = "p.LotSizeSqFt >= :lot_size_min";
            $this->params['lot_size_min'] = $filters['lot_size_min'];
        }

        if ($this->isValidFilterValue($filters['lot_size_max'] ?? null)) {
            $this->whereConditions[] = "p.LotSizeSqFt <= :lot_size_max";
            $this->params['lot_size_max'] = $filters['lot_size_max'];
        }
    }

    private function addYearBuiltFilter($filters)
    {
        if ($this->isValidFilterValue($filters['year_built_min'] ?? null)) {
            $this->whereConditions[] = "p.YearBuilt >= :year_built_min";
            $this->params['year_built_min'] = $filters['year_built_min'];
        }

        if ($this->isValidFilterValue($filters['year_built_max'] ?? null)) {
            $this->whereConditions[] = "p.YearBuilt <= :year_built_max";
            $this->params['year_built_max'] = $filters['year_built_max'];
        }
    }

    private function addStoriesFilter($filters)
    {
        if ($this->isValidFilterValue($filters['stories_min'] ?? null)) {
            $this->whereConditions[] = "p.StoriesNbrCode >= :stories_min";
            $this->params['stories_min'] = $filters['stories_min'];
        }

        if ($this->isValidFilterValue($filters['stories_max'] ?? null)) {
            $this->whereConditions[] = "p.StoriesNbrCode <= :stories_max";
            $this->params['stories_max'] = $filters['stories_max'];
        }
    }

    private function addMarketValueFilter($filters)
    {
        if ($this->isValidFilterValue($filters['estimated_value_min'] ?? null)) {
            $this->whereConditions[] = "p.MarketTotalValue >= :estimated_value_min";
            $this->params['estimated_value_min'] = $filters['estimated_value_min'];
        }

        if ($this->isValidFilterValue($filters['estimated_value_max'] ?? null)) {
            $this->whereConditions[] = "p.MarketTotalValue <= :estimated_value_max";
            $this->params['estimated_value_max'] = $filters['estimated_value_max'];
        }
    }

    private function addAssessedValueFilters($filters)
    {
        if ($this->isValidFilterValue($filters['assessed_total_min'] ?? null)) {
            $this->whereConditions[] = "p.AssdTotalValue >= :assessed_total_min";
            $this->params['assessed_total_min'] = $filters['assessed_total_min'];
        }

        if ($this->isValidFilterValue($filters['assessed_total_max'] ?? null)) {
            $this->whereConditions[] = "p.AssdTotalValue <= :assessed_total_max";
            $this->params['assessed_total_max'] = $filters['assessed_total_max'];
        }

        if ($this->isValidFilterValue($filters['assessed_land_min'] ?? null)) {
            $this->whereConditions[] = "p.AssdLandValue >= :assessed_land_min";
            $this->params['assessed_land_min'] = $filters['assessed_land_min'];
        }

        if ($this->isValidFilterValue($filters['assessed_land_max'] ?? null)) {
            $this->whereConditions[] = "p.AssdLandValue <= :assessed_land_max";
            $this->params['assessed_land_max'] = $filters['assessed_land_max'];
        }

        if ($this->isValidFilterValue($filters['assessed_improvement_min'] ?? null)) {
            $this->whereConditions[] = "p.AssdImprovementValue >= :assessed_improvement_min";
            $this->params['assessed_improvement_min'] = $filters['assessed_improvement_min'];
        }

        if ($this->isValidFilterValue($filters['assessed_improvement_max'] ?? null)) {
            $this->whereConditions[] = "p.AssdImprovementValue <= :assessed_improvement_max";
            $this->params['assessed_improvement_max'] = $filters['assessed_improvement_max'];
        }
    }

    private function addEquityFilters($filters)
    {
        if ($this->isValidFilterValue($filters['equity_percent_min'] ?? null)) {
            $this->whereConditions[] = "(p.MarketTotalValue > 0 AND ((p.MarketTotalValue - COALESCE(p.TotalOpenLienAmt, 0)) / p.MarketTotalValue * 100) >= :equity_percent_min)";
            $this->params['equity_percent_min'] = $filters['equity_percent_min'];
        }

        if ($this->isValidFilterValue($filters['equity_percent_max'] ?? null)) {
            $this->whereConditions[] = "(p.MarketTotalValue > 0 AND ((p.MarketTotalValue - COALESCE(p.TotalOpenLienAmt, 0)) / p.MarketTotalValue * 100) <= :equity_percent_max)";
            $this->params['equity_percent_max'] = $filters['equity_percent_max'];
        }

        if ($this->isValidFilterValue($filters['equity_value_min'] ?? null)) {
            $this->whereConditions[] = "(p.MarketTotalValue - COALESCE(p.TotalOpenLienAmt, 0)) >= :equity_value_min";
            $this->params['equity_value_min'] = $filters['equity_value_min'];
        }

        if ($this->isValidFilterValue($filters['equity_value_max'] ?? null)) {
            $this->whereConditions[] = "(p.MarketTotalValue - COALESCE(p.TotalOpenLienAmt, 0)) <= :equity_value_max";
            $this->params['equity_value_max'] = $filters['equity_value_max'];
        }
    }

    private function addLoanFilters($filters)
    {
        if (!empty($filters['loan_arm'])) {
            $this->whereConditions[] = "(p.ConcurrentMtg1AdjOrFix = 'ADJ' OR p.ConcurrentMtg1LoanType = '3')";
        }

        if (!empty($filters['loan_private_lender'])) {
            $this->whereConditions[] = "(p.Mtg1PrivateLender = 'Y' OR p.Mtg2PrivateLender = 'Y')";
        }

        if (!empty($filters['loan_assumable'])) {
            $this->whereConditions[] = "p.ConcurrentMtg1LoanType IN ('4', '5')";
        }

        if (!empty($filters['loan_fixed_30'])) {
            $this->whereConditions[] = "(p.ConcurrentMtg1LoanType = '20' AND p.ConcurrentMtg1Term = 360)";
        }
    }

    private function addMotivationTagFilters($filters)
    {
        // Add tag filters for each of the 47 tags
        $tagFilters = [
            'tag_negative_equity' => "p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) < 0",
            'tag_low_equity' => "p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) BETWEEN 0 AND 0.20",
            'tag_high_equity' => "((p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL))",
            'tag_no_mortgage' => "(p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL)",
            'tag_high_ltv' => "p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND (p.TotalOpenLienAmt / p.MarketTotalValue) >= 0.80",
            'tag_reverse_mortgage' => "p.ConcurrentMtg1LoanType = '32' AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR)",
            'tag_arm' => "(p.ConcurrentMtg1AdjOrFix = 'ADJ' OR p.ConcurrentMtg1LoanType = '3') AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)",
            'tag_on_market' => "(p.IsListedFlag = 'Y' OR p.IsListedFlag = '1') AND (p.IsListedFlagDate IS NULL OR p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH))",
            'tag_failed_listing' => "p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH) AND (p.CurrentSaleRecordingDate IS NULL OR p.CurrentSaleRecordingDate < p.IsListedFlagDate)",
            'tag_absentee_owner' => "(p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress)",
            'tag_out_of_state_owner' => "p.MailingState != p.SitusState",
            'tag_trust_owned' => "((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%')",
            'tag_corporate_owner' => "(p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\\\b(LLC|INC|CORP|LP|LTD)\\\\b')",
            'tag_elderly_owner' => "(p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)",
            'tag_owner_occupied' => "p.OwnerOccupied = 'Y'",
            'tag_fixer_upper' => "(p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10))",
            'tag_vacant' => "p.VacantFlag IN ('M','B','Y')",
            'tag_tax_delinquent' => "p.TaxDeliquentYear >= YEAR(CURDATE()) - 1",
            'tag_foreclosure' => "((p.CurrentSaleDocumentType IN ('34','64','N') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('34','64','N') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))",
            'tag_pre_foreclosure' => "((p.CurrentSaleDocumentType IN ('77','78','79','81') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)) OR (p.PrevSaleDocumentType IN ('77','78','79','81') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)))",
            'tag_probate' => "((p.CurrentSaleDocumentType IN ('33','19','13') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('33','19','13') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))",
        ];

        foreach ($tagFilters as $tagName => $condition) {
            if (isset($filters[$tagName]) && $filters[$tagName] === 'Y') {
                $this->whereConditions[] = $condition;
            }
        }
    }

    private function addStructureTypeFilter($filters)
    {
        if (!empty($filters['structure_type']) && is_array($filters['structure_type'])) {
            $structureConditions = [];

            // Get the style to property type mapping from DatatreeAPIHelperExtended
            $styleMapping = DatatreeAPIHelperExtended::getStyleMapping();
            $stylePropTypeMapping = DatatreeAPIHelperExtended::getStylePropTypeMapping();

            // Create reverse mapping: property type => array of style codes
            $propTypeToStyleCodes = [];
            foreach ($styleMapping as $styleCode => $styleName) {
                $propType = $stylePropTypeMapping[$styleName] ?? 'Other';
                if (!isset($propTypeToStyleCodes[$propType])) {
                    $propTypeToStyleCodes[$propType] = [];
                }
                $propTypeToStyleCodes[$propType][] = $styleCode;
            }

            foreach ($filters['structure_type'] as $type) {
                $styleCodes = [];

                // Map the incoming structure types to the correct property types
                // Only include the exact property type specified, not related types
                if (isset($propTypeToStyleCodes[$type])) {
                    $styleCodes = array_merge($styleCodes, $propTypeToStyleCodes[$type]);
                }

                // Special handling for Manufactured to include MobileHomeInd
                if ($type === 'Manufactured') {
                    $styleCodes[] = "MobileHomeInd = 'Y'";
                }

                if (!empty($styleCodes)) {
                    // Filter out the MobileHomeInd condition and handle it separately
                    $numericCodes = array_filter($styleCodes, 'is_numeric');
                    $mobileHomeCondition = in_array("MobileHomeInd = 'Y'", $styleCodes);

                    $condition = '';
                    if (!empty($numericCodes)) {
                        $condition = "p.StyleCode IN (" . implode(',', $numericCodes) . ")";
                    }
                    if ($mobileHomeCondition) {
                        if (!empty($condition)) {
                            $condition = "(" . $condition . " OR p.MobileHomeInd = 'Y')";
                        } else {
                            $condition = "p.MobileHomeInd = 'Y'";
                        }
                    }

                    if (!empty($condition)) {
                        $structureConditions[] = $condition;
                    }
                }
            }

            if (!empty($structureConditions)) {
                $condition = "(" . implode(" OR ", $structureConditions) . ")";
                $this->whereConditions[] = $condition;
            }
        }
    }

    private function addOwnerOccupiedFilter($filters)
    {
        if (isset($filters['owner_occupied']) && $filters['owner_occupied'] !== '') {
            if ($filters['owner_occupied'] === 'Y') {
                $this->whereConditions[] = "p.OwnerOccupied = 'Y'";
            } else {
                $this->whereConditions[] = "p.OwnerOccupied != 'Y'";
            }
        }
    }

    private function addTagBasedFilters($filters)
    {
        // Define all tag mappings
        $tagMappings = [
            // Equity Tags
            'negative_equity' => "p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) < 0",
            'high_ltv' => "p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND (p.TotalOpenLienAmt / p.MarketTotalValue) >= 0.80",
            'arm' => "(p.ConcurrentMtg1AdjOrFix = 'ADJ' OR p.ConcurrentMtg1LoanType = '3') AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)",
            'high_equity' => "((p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) >= 0.50) OR (p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL))",
            'no_mortgage' => "(p.TotalOpenLienAmt = 0 OR p.TotalOpenLienAmt IS NULL)",
            'reverse_mortgage' => "p.ConcurrentMtg1LoanType = '32' AND p.ConcurrentMtg1RecordingDate >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR)",
            'low_equity' => "p.MarketTotalValue > 0 AND p.TotalOpenLienAmt > 0 AND ((p.MarketTotalValue - p.TotalOpenLienAmt) / p.MarketTotalValue) BETWEEN 0 AND 0.20",

            // Market Tags
            'historical_sale' => "p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 12 MONTH)",
            'on_market' => "(p.IsListedFlag = 'Y' OR p.IsListedFlag = '1') AND (p.IsListedFlagDate IS NULL OR p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH))",
            'ancient_sale' => "p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 15 YEAR)",
            'below_market_sale' => "p.CurrentSalesPrice > 0 AND p.MarketTotalValue > 0 AND (p.CurrentSalesPrice / p.MarketTotalValue) < 0.85 AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)",
            'recent_sale' => "p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)",
            'flip_potential' => "p.BuildingConditionCode IN (4,5) AND p.MarketTotalValue > 0 AND p.AssdLandValue > 0 AND (p.AssdLandValue / p.MarketTotalValue) >= 0.60",
            'failed_listing' => "p.IsListedFlagDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH) AND (p.CurrentSaleRecordingDate IS NULL OR p.CurrentSaleRecordingDate < p.IsListedFlagDate)",
            'legacy_sale' => "p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 15 YEAR) AND p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 5 YEAR)",

            // Ownership Tags
            'absentee_owner' => "(p.OwnerOccupied != 'Y' OR p.SitusFullStreetAddress != p.MailingFullStreetAddress)",
            'elderly_owner' => "(p.SeniorInd = 'Y' OR p.HomesteadInd = 'Y' OR p.Owner1Suffix LIKE '%SR%') AND (p.CurrentSaleRecordingDate IS NULL OR YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) >= 10)",
            'corporate_owner' => "(p.Owner1CorpInd = 'T' OR p.Owner2CorpInd = 'T' OR p.OwnerNAME1FULL REGEXP '\\\\b(LLC|INC|CORP|LP|LTD)\\\\b')",
            'trust_owned' => "((p.CurrentSaleDocumentType = 'DT' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR (p.PrevSaleDocumentType = 'DT' AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)) OR p.OwnerNAME1FULL LIKE '%TRUST%')",
            'out_of_state_owner' => "p.MailingState != p.SitusState",
            'owner_occupied' => "p.OwnerOccupied = 'Y'",

            // Condition Tags
            'fixer_upper' => "(p.BuildingConditionCode IN (4,5) OR (p.YearBuilt < 1975 AND COALESCE(p.EffectiveYearBuilt, p.YearBuilt) < YEAR(CURDATE()) - 10))",
            'multi_unit' => "p.SumResidentialUnits > 1 OR (p.PropertyClassID = 'R' AND p.SumBuildingsNbr > 1)",
            'pool_present' => "p.PoolCode IS NOT NULL AND p.PoolCode != '0' AND p.PoolCode != ''",
            'aging_property' => "p.YearBuilt > 0 AND YEAR(CURDATE()) - p.YearBuilt >= 40",
            'large_lot' => "p.LotSizeAcres >= 1.0 OR (p.LotSizeSqFt >= 21780 AND p.PropertyClassID = 'R')",
            'value_add' => "p.MarketTotalValue > 0 AND p.AssdImprovementValue > 0 AND (p.AssdImprovementValue / p.MarketTotalValue) < 0.30 AND p.LotSizeAcres >= 0.25",
            'commercial_potential' => "p.PropertyClassID = 'R' AND p.Zoning REGEXP '(C-|COM|COMM|BUSINESS|RETAIL|OFFICE|INDUSTRIAL|MIX)'",

            // Risk Tags
            'flood_zone' => "p.flInsideSFHA = 'Y' OR p.flFemaFloodZone REGEXP '[AEV]'",
            'basement_present' => "p.BasementCode IS NOT NULL AND p.BasementCode NOT IN ('5', '')",
            'view_amenities_premium' => "p.Amenities LIKE '%B%' OR p.Amenities LIKE '%L%' OR p.Amenities LIKE '%W%' OR p.SiteInfluenceCode IN ('1', '2', '3', '4', '5', '6', '7', '8', '10', '11', '12')",
            'hoa_property' => "p.HOA1Name IS NOT NULL AND p.HOA1Name != ''",
            'historical_property' => "p.StyleCode = 27 OR p.StyleCode = 57",
            'mobile_home' => "p.MobileHomeInd = 'Y' OR p.StyleCode IN (33, 45)",

            // Distress Tags
            'pre_foreclosure' => "((p.CurrentSaleDocumentType IN ('77','78','79','81') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)) OR (p.PrevSaleDocumentType IN ('77','78','79','81') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 18 MONTH)))",
            'vacant' => "p.VacantFlag IN ('M','B','Y')",
            'exemptions_indigency' => "(p.WelfareInd = 'Y' OR p.DisabledInd = 'Y' OR p.WidowInd = 'Y' OR p.VeteranInd = 'Y')",
            'potential_tax_issues' => "p.TaxAmt > 0 AND p.TaxYear < YEAR(CURDATE()) - 2",
            'special_document' => "p.CurrentSaleDocumentType BETWEEN '27' AND '50' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)",
            'foreclosure' => "((p.CurrentSaleDocumentType IN ('34','64','N') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('34','64','N') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.CurrentSaleSeller1FullName LIKE '%BANK%' AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))",
            'tax_delinquent' => "p.TaxDeliquentYear >= YEAR(CURDATE()) - 1",
            'long_term_ownership' => "YEAR(CURDATE()) - YEAR(p.CurrentSaleRecordingDate) > 20",
            'probate' => "((p.CurrentSaleDocumentType IN ('33','19','13') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('33','19','13') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))",
            'pre_probate' => "((p.CurrentSaleDocumentType IN ('29','M') AND p.CurrentSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)) OR (p.PrevSaleDocumentType IN ('29','M') AND p.PrevSaleRecordingDate >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)))",
            'low_confidence_value' => "p.vConfidenceScore > 0 AND p.vConfidenceScore < 50 AND p.vValuationDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)",
            'low_market_value' => "p.MarketTotalValue > 0 AND p.AssdTotalValue > 0 AND (p.MarketTotalValue / p.AssdTotalValue) < 0.70",
            'no_recent_activity' => "p.CurrentSaleRecordingDate < DATE_SUB(CURDATE(), INTERVAL 10 YEAR)",
        ];

        // Process each tag category
        $tagCategories = ['equity_tags', 'market_tags', 'ownership_tags', 'condition_tags', 'risk_tags', 'distress_tags'];

        foreach ($tagCategories as $category) {
            if (!empty($filters[$category]) && is_array($filters[$category])) {
                $categoryConditions = [];

                foreach ($filters[$category] as $tag) {
                    if (isset($tagMappings[$tag])) {
                        $categoryConditions[] = $tagMappings[$tag];
                    }
                }

                if (!empty($categoryConditions)) {
                    $this->whereConditions[] = "(" . implode(" OR ", $categoryConditions) . ")";
                }
            }
        }
    }

    /**
     * Build ORDER BY clause from order_by parameter
     * Supports formats like:
     * - "property_id"
     * - "sell_date_low_high" or "sell_date_high_low"
     * - "bedroom_low_high" or "bedroom_high_low"
     * - "bathroom_low_high" or "bathroom_high_low"
     * - "year_built_low_high" or "year_built_high_low"
     * - "sqft_low_high" or "sqft_high_low"
     */
    private function buildOrderByClause($orderBy)
    {
        // Column mapping
        $columnMap = [
            'property_id' => 'p.PropertyID',
            'sell_date' => 'p.CurrentSaleRecordingDate',
            'bedroom' => 'p.Bedrooms',
            'bathroom' => 'p.BathTotalCalc',
            'year_built' => 'p.YearBuilt',
            'sqft' => 'p.BuildingArea'
        ];

        // Check if order_by contains direction (low_high or high_low)
        if (strpos($orderBy, '_low_high') !== false) {
            $field = str_replace('_low_high', '', $orderBy);
            $direction = 'ASC';
        } elseif (strpos($orderBy, '_high_low') !== false) {
            $field = str_replace('_high_low', '', $orderBy);
            $direction = 'DESC';
        } else {
            // No direction specified, default to ASC for property_id, DESC for dates
            $field = $orderBy;
            if ($orderBy === 'property_id') {
                $direction = 'ASC';
            } elseif ($orderBy === 'sell_date') {
                $direction = 'DESC'; // Most recent first
            } else {
                $direction = 'ASC'; // Default ascending
            }
        }

        // Get the column name
        if (isset($columnMap[$field])) {
            return $columnMap[$field] . ' ' . $direction;
        }

        // Fallback: if field matches a column directly (like PropertyID)
        if (preg_match('/^[A-Z][a-zA-Z0-9]*$/', $field)) {
            return 'p.' . $field . ' ' . ($direction ?? 'ASC');
        }

        // Default fallback
        return 'p.PropertyID ASC';
    }

    private function getFinalQuery($filters = [])
    {
        // Insert WHERE conditions before ORDER BY
        $sql = $this->baseQuery;

        // Find the position to insert additional WHERE conditions
        // Insert before the ORDER BY clause
        $orderByPos = strpos($sql, 'ORDER BY');

        if (!empty($this->whereConditions)) {
            $additionalWhere = "\n  AND " . implode("\n  AND ", $this->whereConditions) . "\n  ";
            $sql = substr_replace($sql, $additionalWhere, $orderByPos, 0);
        }

        // Handle LIMIT - use default limit, enforce max limit
        $limit = $this->defaultLimit;
        if (isset($filters['_limit']) && $filters['_limit'] !== '' && $filters['_limit'] !== null) {
            $limit = intval($filters['_limit']);
            // Enforce maximum limit
            if ($limit > $this->maxLimit) {
                $limit = $this->maxLimit;
            }
            // Enforce minimum limit of 1
            if ($limit < 1) {
                $limit = $this->defaultLimit;
            }
        }

        // Handle custom ORDER BY with direction support
        if (!empty($filters['order_by'])) {
            $orderByClause = $this->buildOrderByClause($filters['order_by']);
            if ($orderByClause) {
                // Replace ORDER BY line, but keep everything after it (LIMIT)
                $sql = preg_replace('/ORDER BY[^\n]*/i', "ORDER BY {$orderByClause}", $sql);
            }
        }

        // Handle LIMIT - replace if exists, add if not
        // Remove any existing LIMIT clause first
        $sql = preg_replace('/\s*LIMIT\s+\d+\s*;?\s*$/mi', '', $sql);
        // Add the LIMIT clause
        $sql = rtrim($sql, " \n\r\t\v\0;") . "\nLIMIT {$limit};";

        return $sql;
    }
}

// ===================================================================
// MAIN EXECUTION - STREAMING VERSION
// ===================================================================

// Initialize
$queryBuilder = new InvestorQueryBuilder();

// Get filters from POST
$filters = $_POST;

// Filter out ignorable fields
$ignorableFields = [
    'isDirty',
    'errors',
    'hasErrors',
    'processing',
    'progress',
    'wasSuccessful',
    'recentlySuccessful',
    '__rememberable',
    'comps_sub_prop_id',
    'savedSearchAsNew',
    'savedSearchId',
    'savedSearchName'
];

foreach ($ignorableFields as $field) {
    unset($filters[$field]);
}

// Normalize min/max numeric filters - convert 0, '0', null, or empty to null
$numericMinMaxFields = [
    'bedrooms_min',
    'bedrooms_max',
    'bathrooms_min',
    'bathrooms_max',
    'building_area_min',
    'building_area_max',
    'lot_size_min',
    'lot_size_max',
    'year_built_min',
    'year_built_max',
    'estimated_value_min',
    'estimated_value_max',
    'equity_percent_min',
    'equity_percent_max',
    'years_owned_min',
    'years_owned_max',
    'assessed_total_min',
    'assessed_total_max',
    'assessed_land_min',
    'assessed_land_max',
    'assessed_improvement_min',
    'assessed_improvement_max',
    'equity_value_min',
    'equity_value_max',
    'sale_price_min',
    'sale_price_max'
];

foreach ($numericMinMaxFields as $field) {
    if (isset($filters[$field])) {
        $value = $filters[$field];

        // Normalize the value - trim if string
        if (is_string($value)) {
            $value = trim($value);
            $filters[$field] = $value; // Update with trimmed value
        }

        // Convert 0, '0', 0.0, null, empty string to unset (which will be ignored)
        // Use loose comparison for numeric values to catch all zero variants
        $shouldUnset = false;

        if ($value === null || $value === '') {
            $shouldUnset = true;
        } elseif ($value === 0 || $value === '0' || $value === 0.0) {
            $shouldUnset = true;
        } elseif (is_numeric($value)) {
            // For any numeric value, check if it equals zero
            if ((float) $value == 0) {
                $shouldUnset = true;
            }
        }

        if ($shouldUnset) {
            unset($filters[$field]);
        }
    }
}

// Normalize array fields - convert comma-separated strings back to arrays
// PHP converts arrays to comma-separated strings in form submissions
$arrayFields = [
    'structure_type',
    'owner_type',
    'property_ids',
    'equity_tags',
    'market_tags',
    'ownership_tags',
    'condition_tags',
    'risk_tags',
    'distress_tags'
];

foreach ($arrayFields as $field) {
    if (isset($filters[$field]) && is_string($filters[$field]) && !empty($filters[$field])) {
        // Convert comma-separated string to array
        $filters[$field] = array_map('trim', explode(',', $filters[$field]));
    } elseif (isset($filters[$field]) && empty($filters[$field])) {
        // Convert empty strings to empty arrays
        $filters[$field] = [];
    }
}

// Normalize _limit to ensure it's an integer
if (isset($filters['_limit'])) {
    if (is_string($filters['_limit']) && $filters['_limit'] !== '') {
        $filters['_limit'] = intval($filters['_limit']);
    } elseif (empty($filters['_limit']) || $filters['_limit'] === '') {
        unset($filters['_limit']); // Remove empty limit to use default
    }
}

// Validate required fields
// County is required only if property_ids is empty
if (empty($filters['county']) && empty($filters['property_ids'])) {
    // For streaming, we need to output JSON error and exit
    header('Content-Type: application/json');
    echo json_encode([
        'status' => '0',
        'message' => 'County FIPS is required when property_ids is not provided',
        'data' => [],
        'count' => 0
    ]);
    exit;
}

// Build query
$sql = $queryBuilder->buildQuery($filters);
$params = $queryBuilder->getParams();

// // Set up streaming output
// // Disable all output buffering for true streaming
// while (ob_get_level()) {
//     ob_end_clean();
// }

// // Set headers for streaming JSON
// header('Content-Type: application/json');
// header('Cache-Control: no-store, no-cache, must-revalidate');
// header('X-Accel-Buffering: no'); // Disable nginx buffering
// // Note: We don't set Transfer-Encoding: chunked manually
// // The web server will handle chunking automatically if needed

// // Output initial JSON structure
// echo '{"status":"1","message":"Streaming results...","data":[';

// // Flush headers and initial output immediately
// flush();

// // Execute with PDO and stream results
// try {
//     $pdo = new PDO($dsn, $username, $password);
//     // PDO fetches rows one at a time by default when using fetch() in a loop
//     // This is important for memory efficiency with large result sets

//     $stmt = $pdo->prepare($sql);
//     $stmt->execute($params);

//     $firstRow = true;
//     $rowCount = 0;
//     $chunkSize = 100; // Flush every 100 rows

//     // Fetch and stream rows one at a time
//     while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
//         // Transform the row
//         $transformedRow = DatatreeAPIHelper::transformPropertyData($row);

//         // Output comma before each row except the first
//         if (!$firstRow) {
//             echo ',';
//         }
//         $firstRow = false;

//         // Output the JSON-encoded row
//         echo json_encode($transformedRow, JSON_PARTIAL_OUTPUT_ON_ERROR | JSON_UNESCAPED_UNICODE);

//         $rowCount++;

//         // Flush output every chunkSize rows to keep memory usage low
//         if ($rowCount % $chunkSize === 0) {
//             flush();
//         }
//     }

//     // Close the JSON structure (handles both empty and non-empty results)
//     echo '],"count":' . $rowCount . '}';

//     // Final flush
//     flush();

// } catch (Exception $e) {
//     // On error, output error JSON
//     // Clear any partial output
//     while (ob_get_level()) {
//         ob_end_clean();
//     }

//     // Reset headers if possible
//     if (!headers_sent()) {
//         header('Content-Type: application/json');
//     }

//     echo json_encode([
//         'status' => '0',
//         'message' => 'Database error: ' . $e->getMessage(),
//         'data' => [],
//         'count' => 0
//     ]);
//     exit;
// }

// Set up streaming output
// Disable all output buffering for true streaming
while (ob_get_level()) {
    ob_end_clean();
}

// Set headers for streaming JSON
header('Content-Type: application/json');
header('Cache-Control: no-store, no-cache, must-revalidate');
header('X-Accel-Buffering: no'); // Disable nginx buffering

// Don't start with opening JSON - we'll send complete objects

// Flush headers immediately
flush();

// Execute with PDO and stream results
try {
    $pdo = new PDO($dsn, $username, $password);

    $stmt = $pdo->prepare($sql);
    $stmt->execute($params);

    $rowCount = 0;
    $chunkSize = 5000; // Send every 100 rows as a complete JSON object
    $chunk = []; // Accumulate rows for current chunk

    // Fetch and stream rows one at a time
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        // Transform the row
        $transformedRow = DatatreeAPIHelper::transformPropertyData($row);

        // Build Tags array from tag_* fields
        $transformedRow['Tags'] = InvestorQueryBuilder::buildTagsFromRow($row);

        // Remove all tag_* fields from response (they're not needed, only Tags array is)
        foreach ($transformedRow as $key => $value) {
            if (strpos($key, 'tag_') === 0) {
                unset($transformedRow[$key]);
            }
        }

        // Add to current chunk
        $chunk[] = $transformedRow;
        $rowCount++;

        // When chunk is full, send it as a complete JSON object
        if (count($chunk) >= $chunkSize) {
            // Send complete JSON object
            echo json_encode([
                'status' => '1',
                'message' => 'Streaming...',
                'data' => $chunk,
                'count' => $rowCount // Total count so far
            ], JSON_PARTIAL_OUTPUT_ON_ERROR | JSON_UNESCAPED_UNICODE);

            // Flush immediately so client receives this chunk
            flush();

            // Clear chunk for next batch
            $chunk = [];

            // Optional: tiny sleep to ensure chunk boundary (helps with buffering issues)
            usleep(1000); // 1ms
        }
    }

    // Send any remaining rows as final chunk
    if (count($chunk) > 0) {
        echo json_encode([
            'status' => '1',
            'message' => 'Complete',
            'data' => $chunk,
            'count' => $rowCount
        ], JSON_PARTIAL_OUTPUT_ON_ERROR | JSON_UNESCAPED_UNICODE);

        flush();
    } else if ($rowCount === 0) {
        // Handle empty result set
        echo json_encode([
            'status' => '1',
            'message' => 'No results found',
            'data' => [],
            'count' => 0
        ]);

        flush();
    }

} catch (Exception $e) {
    // On error, output error JSON
    // Clear any partial output
    while (ob_get_level()) {
        ob_end_clean();
    }

    // Reset headers if possible
    if (!headers_sent()) {
        header('Content-Type: application/json');
    }

    echo json_encode([
        'status' => '0',
        'message' => 'Database error: ' . $e->getMessage(),
        'data' => [],
        'count' => 0
    ]);

    flush();
    exit;
}
