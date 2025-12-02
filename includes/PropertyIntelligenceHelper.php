<?php

/**
 * Property Intelligence Helper
 * This class analyzes property data and assigns intelligence tags based on
 * six categories: Ownership, Equity, Market, Condition, Risk, Distress
 * Each property gets multiple tags, but only the highest priority one shows on main screen
 */

class PropertyIntelligenceHelper
{
    /**
     * Analyze property and return all intelligence tags
     * @param array $propertyData
     * @param DateTime $currentDate
     * @return array
     */
    public static function analyzeProperty($propertyData, $currentDate)
    {
        $allTags = [];
        
        // Get tags from each category with time sensitivity
        $allTags = array_merge($allTags, self::getEquityTags($propertyData, $currentDate));
        $allTags = array_merge($allTags, self::getMarketTags($propertyData, $currentDate));
        $allTags = array_merge($allTags, self::getOwnershipTags($propertyData, $currentDate));
        $allTags = array_merge($allTags, self::getConditionTags($propertyData, $currentDate));
        $allTags = array_merge($allTags, self::getRiskTags($propertyData, $currentDate));
        $allTags = array_merge($allTags, self::getDistressTags($propertyData, $currentDate));
        
        // Sort by priority (lower number = higher priority)
        usort($allTags, function($a, $b) {
            return $a['priority'] - $b['priority'];
        });
        
        // Get the highest priority tag (primary) and others (secondary)
        // If no tags exist, return null for both
        if (empty($allTags)) {
            $primaryTag = null;
            $secondaryTags = [];
        } else {
            $primaryTag = $allTags[0];
            $secondaryTags = array_slice($allTags, 1);
        }
        
        // Ensure all 6 categories are always present in the response
        $categories = ['Equity', 'Market', 'Ownership', 'Condition', 'Risk', 'Distress'];
        $tagsByCategory = [];
        
        // Group existing tags by category
        foreach ($allTags as $tag) {
            $tagsByCategory[$tag['category']][] = $tag;
        }
        
        // Ensure all categories exist (even if empty)
        foreach ($categories as $category) {
            if (!isset($tagsByCategory[$category])) {
                $tagsByCategory[$category] = [];
            }
        }
        
        return [
            'primary_tag' => $primaryTag,
            'secondary_tags' => $secondaryTags,
            'all_tags' => $allTags,
            'categories' => $tagsByCategory
        ];
    }
    
    /**
     * Equity and Financial Flags
     */
    private static function getEquityTags($data, $currentDate)
    {
        $tags = [];
        
        // Check for No Mortgage/Free and Clear (Priority 4) - Current, no time sensitivity
        if (empty($data['TotalOpenLienNbr']) || $data['TotalOpenLienNbr'] == 0 || 
            empty($data['TotalOpenLienAmt']) || $data['TotalOpenLienAmt'] == 0) {
            $tags[] = [
                'category' => 'Equity',
                'flag' => 'No Mortgage/Free and Clear',
                'priority' => 4,
                'level' => 1,
                'description' => 'Debt-free properties allow flexibility'
            ];
        }
        
        // Check for Reverse Mortgage (Priority 6) - If recording date within 10 years
        if (isset($data['ConcurrentMtg1LoanType']) && $data['ConcurrentMtg1LoanType'] == '32') {
            $mtgDateStr = $data['ConcurrentMtg1RecordingDate'] ?? null;
            if ($mtgDateStr) {
                $mtgDate = new DateTime($mtgDateStr);
                $yearsAgo = $currentDate->diff($mtgDate)->y;
                if ($yearsAgo <= 10) {
                    $tags[] = [
                        'category' => 'Equity',
                        'flag' => 'Reverse Mortgage',
                        'priority' => 6,
                        'level' => 1,
                        'description' => 'Reverse mortgage often tied to elderly owners'
                    ];
                }
            }
        }
        
        // Check for Adjustable Rate Mortgage (Priority 7) - If within 5 years
        if ((isset($data['ConcurrentMtg1AdjOrFix']) && $data['ConcurrentMtg1AdjOrFix'] == 'ADJ') ||
            (isset($data['ConcurrentMtg1LoanType']) && $data['ConcurrentMtg1LoanType'] == '3')) {
            $mtgDateStr = $data['ConcurrentMtg1RecordingDate'] ?? null;
            if ($mtgDateStr) {
                $mtgDate = new DateTime($mtgDateStr);
                $yearsAgo = $currentDate->diff($mtgDate)->y;
                if ($yearsAgo <= 5) {
                    $tags[] = [
                        'category' => 'Equity',
                        'flag' => 'Adjustable Rate Mortgage (ARM)',
                        'priority' => 7,
                        'level' => 1,
                        'description' => 'ARMs can lead to payment shocks'
                    ];
                }
            }
        }
        
        // Calculate equity-based tags (Level 2 - requires AVM/ARV)
        // Use MarketTotalValue if CurrentAVMValue is 0 or null
        $currentValue = $data['CurrentAVMValue'] ?? 0;
        if ($currentValue == 0) {
            $currentValue = $data['MarketTotalValue'] ?? 0;
        }
        
        // Amortize total lien amount if mortgage date is available and old
        $totalLienAmount = $data['TotalOpenLienAmt'] ?? 0;
        $mtgDateStr = $data['Mtg1RecordingDate'] ?? null;  // Assuming primary mortgage
        if ($mtgDateStr && $totalLienAmount > 0 && isset($data['Mtg1Term']) && isset($data['Mtg1InterestRate'])) {
            $mtgDate = new DateTime($mtgDateStr);
            $yearsPassed = $currentDate->diff($mtgDate)->y;
            if ($yearsPassed > 2) {  // Amortize if >2 years old
                $annualRate = (float)$data['Mtg1InterestRate'];
                $termYears = (int)($data['Mtg1Term'] / 12);  // Assuming term in months
                $totalLienAmount = self::calculateRemainingBalance($totalLienAmount, $annualRate, $termYears, $yearsPassed);
            }
        }
        
        if ($currentValue > 0) {
            // Check valuation date recency
            $valDateStr = $data['vValuationDate'] ?? null;
            $isRecentVal = true;
            if ($valDateStr) {
                $valDate = new DateTime($valDateStr);
                $monthsAgo = ($currentDate->diff($valDate)->y * 12) + $currentDate->diff($valDate)->m;
                $isRecentVal = $monthsAgo <= 12;
            }
            
            if ($isRecentVal) {
                $equity = $currentValue - $totalLienAmount;
                $equityPercentage = ($equity / $currentValue) * 100;
                $ltv = ($totalLienAmount / $currentValue) * 100;
                
                // Negative Equity (Priority 1)
                if ($totalLienAmount > $currentValue) {
                    $tags[] = [
                        'category' => 'Equity',
                        'flag' => 'Negative Equity',
                        'priority' => 1,
                        'level' => 2,
                        'description' => 'Underwater properties',
                        'equity_amount' => $equity,
                        'equity_percentage' => round($equityPercentage, 2)
                    ];
                }
                // Low Equity (Priority 2)
                elseif ($equityPercentage < 20) {
                    $tags[] = [
                        'category' => 'Equity',
                        'flag' => 'Low Equity',
                        'priority' => 2,
                        'level' => 2,
                        'description' => 'Potential short sales if distressed',
                        'equity_amount' => $equity,
                        'equity_percentage' => round($equityPercentage, 2)
                    ];
                }
                // High Equity (Priority 3)
                elseif ($equityPercentage > 50 || $equity > 100000) {
                    $tags[] = [
                        'category' => 'Equity',
                        'flag' => 'High Equity',
                        'priority' => 3,
                        'level' => 2,
                        'description' => 'High-equity owners can sell without loans',
                        'equity_amount' => $equity,
                        'equity_percentage' => round($equityPercentage, 2)
                    ];
                }
                // High LTV (Priority 5)
                elseif ($ltv > 80) {
                    $tags[] = [
                        'category' => 'Equity',
                        'flag' => 'High Loan-to-Value (LTV)',
                        'priority' => 5,
                        'level' => 2,
                        'description' => 'Indicates refi or distress',
                        'ltv_percentage' => round($ltv, 2)
                    ];
                }
            }
        }
        
        return $tags;
    }
    
    /**
     * Market and Listing Flags
     */
    private static function getMarketTags($data, $currentDate)
    {
        $tags = [];
        
        // On Market (Priority 1) - If flag date within 6 months
        if (isset($data['IsListedFlag']) && ($data['IsListedFlag'] == 'Y' || $data['IsListedFlag'] == '1')) {
            $listedDateStr = $data['IsListedFlagDate'] ?? null;
            $isRecent = true;
            if ($listedDateStr) {
                $listedDate = new DateTime($listedDateStr);
                $monthsAgo = ($currentDate->diff($listedDate)->y * 12) + $currentDate->diff($listedDate)->m;
                $isRecent = $monthsAgo <= 6;
            }
            if ($isRecent) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'On Market',
                    'priority' => 1,
                    'level' => 1,
                    'description' => 'Active listings show intent to sell',
                    'listed_date' => $data['IsListedFlagDate'] ?? null,
                    'price_range' => $data['IsListedPriceRange'] ?? null
                ];
            }
        }
        
        // Recent Sale (Priority 4) - Within 12 months
        if (isset($data['CurrentSaleRecordingDate']) && !empty($data['CurrentSaleRecordingDate'])) {
            $saleDate = new DateTime($data['CurrentSaleRecordingDate']);
            $monthsAgo = ($currentDate->diff($saleDate)->y * 12) + $currentDate->diff($saleDate)->m;
            if ($monthsAgo <= 12) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Recent Sale',
                    'priority' => 4,
                    'level' => 1,
                    'description' => 'Recent transactions show trends',
                    'sale_date' => $data['CurrentSaleRecordingDate'],
                    'sale_price' => $data['CurrentSalesPrice'] ?? null
                ];
            }
        }
        
        // Historical Sale (Priority 4) - Between 1-5 years
        if (isset($data['CurrentSaleRecordingDate']) && !empty($data['CurrentSaleRecordingDate'])) {
            $saleDate = new DateTime($data['CurrentSaleRecordingDate']);
            $yearsAgo = $currentDate->diff($saleDate)->y;
            if ($yearsAgo > 1 && $yearsAgo <= 5) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Historical Sale',
                    'priority' => 4,
                    'level' => 1,
                    'description' => 'Historical sales provide market context',
                    'sale_date' => $data['CurrentSaleRecordingDate'],
                    'sale_price' => $data['CurrentSalesPrice'] ?? null
                ];
            }
        }
        
        // Legacy Sale (Priority 4) - Between 5-20 years
        if (isset($data['CurrentSaleRecordingDate']) && !empty($data['CurrentSaleRecordingDate'])) {
            $saleDate = new DateTime($data['CurrentSaleRecordingDate']);
            $yearsAgo = $currentDate->diff($saleDate)->y;
            if ($yearsAgo > 5 && $yearsAgo <= 20) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Legacy Sale',
                    'priority' => 4,
                    'level' => 1,
                    'description' => 'Legacy sales show long-term market history',
                    'sale_date' => $data['CurrentSaleRecordingDate'],
                    'sale_price' => $data['CurrentSalesPrice'] ?? null
                ];
            }
        }
        
        // Ancient Sale (Priority 4) - Older than 20 years
        if (isset($data['CurrentSaleRecordingDate']) && !empty($data['CurrentSaleRecordingDate'])) {
            $saleDate = new DateTime($data['CurrentSaleRecordingDate']);
            $yearsAgo = $currentDate->diff($saleDate)->y;
            if ($yearsAgo > 20) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Ancient Sale',
                    'priority' => 4,
                    'level' => 1,
                    'description' => 'Ancient sales show historical market activity',
                    'sale_date' => $data['CurrentSaleRecordingDate'],
                    'sale_price' => $data['CurrentSalesPrice'] ?? null
                ];
            }
        }
        
        // Below Market Sale (Priority 3) - If recent (within 12 months)
        $currentValue = $data['CurrentAVMValue'] ?? 0;
        if ($currentValue == 0) {
            $currentValue = $data['MarketTotalValue'] ?? 0;
        }
        $salePrice = $data['CurrentSalesPrice'] ?? 0;
        $saleDateStr = $data['CurrentSaleRecordingDate'] ?? null;
        if ($currentValue > 0 && $salePrice > 0 && $saleDateStr) {
            $saleDate = new DateTime($saleDateStr);
            $monthsAgo = ($currentDate->diff($saleDate)->y * 12) + $currentDate->diff($saleDate)->m;
            if ($monthsAgo <= 12 && $salePrice < ($currentValue * 0.8)) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Below Market Sale',
                    'priority' => 3,
                    'level' => 1,
                    'description' => 'Discounted sales could indicate distress',
                    'sale_price' => $salePrice,
                    'market_value' => $currentValue,
                    'discount_percentage' => round((($currentValue - $salePrice) / $currentValue) * 100, 2)
                ];
            }
        }
        
        // Failed Listing (Priority 2) - If listed within 12 months and no sale after
        if (isset($data['IsListedFlagDate']) && !empty($data['IsListedFlagDate'])) {
            $listedDate = new DateTime($data['IsListedFlagDate']);
            $monthsAgo = ($currentDate->diff($listedDate)->y * 12) + $currentDate->diff($listedDate)->m;
            $saleDateStr = $data['CurrentSaleRecordingDate'] ?? null;
            $saleDate = $saleDateStr ? new DateTime($saleDateStr) : null;
            
            if ($monthsAgo <= 12 && (!$saleDate || $saleDate <= $listedDate)) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Failed Listing',
                    'priority' => 2,
                    'level' => 2,
                    'description' => 'Expired listings suggest motivation',
                    'listed_date' => $data['IsListedFlagDate'],
                    'price_range' => $data['IsListedPriceRange'] ?? null
                ];
            }
        }
        
        // Flip Potential (Priority 5) - If hold <24 months
        if (isset($data['CurrentSalesPrice']) && isset($data['PrevSalesPrice']) && 
            isset($data['CurrentSaleRecordingDate']) && isset($data['PrevSaleRecordingDate']) &&
            $data['CurrentSalesPrice'] > 0 && $data['PrevSalesPrice'] > 0) {
            
            $currentPrice = $data['CurrentSalesPrice'];
            $prevPrice = $data['PrevSalesPrice'];
            $currentDateDt = new DateTime($data['CurrentSaleRecordingDate']);
            $prevDate = new DateTime($data['PrevSaleRecordingDate']);
            
            $holdTime = $currentDateDt->diff($prevDate)->days;
            $profit = $currentPrice - $prevPrice;
            $profitPercentage = ($profit / $prevPrice) * 100;
            $annualReturn = ($profitPercentage / max($holdTime / 365, 0.1)) * 100;
            
            $monthsAgo = ($currentDate->diff($currentDateDt)->y * 12) + $currentDate->diff($currentDateDt)->m;
            if ($monthsAgo <= 24 && (($annualReturn > 20 && $holdTime < 730) || ($profitPercentage > 20 && $holdTime < 730))) {
                $tags[] = [
                    'category' => 'Market',
                    'flag' => 'Flip Potential',
                    'priority' => 5,
                    'level' => 2,
                    'description' => 'Flip history suggests rehab needed',
                    'profit' => $profit,
                    'profit_percentage' => round($profitPercentage, 2),
                    'hold_days' => $holdTime,
                    'annual_return' => round($annualReturn, 2)
                ];
            }
        }
        
        return $tags;
    }
    
    /**
     * Ownership and Occupancy Flags
     */
    private static function getOwnershipTags($data, $currentDate)
    {
        $tags = [];
        
        // Absentee Owner (Priority 1) - No time sensitivity
        if ((empty($data['OwnerOccupied']) || $data['OwnerOccupied'] == 'N') ||
            (isset($data['SitusFullStreetAddress']) && isset($data['MailingFullStreetAddress']) &&
             $data['SitusFullStreetAddress'] != $data['MailingFullStreetAddress'])) {
            $tags[] = [
                'category' => 'Ownership',
                'flag' => 'Absentee Owner',
                'priority' => 1,
                'level' => 1,
                'description' => 'Absentee owners may be more open to selling',
                'owner_occupied' => $data['OwnerOccupied'] ?? null,
                'situs_address' => $data['SitusFullStreetAddress'] ?? null,
                'mailing_address' => $data['MailingFullStreetAddress'] ?? null
            ];
        }
        
        // Out-of-State Owner (Priority 2) - No time
        if (isset($data['MailingState']) && isset($data['SitusState']) && 
            $data['MailingState'] != $data['SitusState']) {
            $tags[] = [
                'category' => 'Ownership',
                'flag' => 'Out-of-State Owner',
                'priority' => 2,
                'level' => 1,
                'description' => 'Out-of-state owners often face management challenges',
                'situs_state' => $data['SitusState'] ?? null,
                'mailing_state' => $data['MailingState'] ?? null
            ];
        }
        
        // Trust-Owned (Priority 3) - If recent (within 5 years)
        $isTrust = false;
        $trustDate = null;
        if ((isset($data['CurrentSaleDocumentType']) && in_array($data['CurrentSaleDocumentType'], ['DT']))) {
            $isTrust = true;
            $trustDate = $data['CurrentSaleRecordingDate'] ?? null;
        } elseif ((isset($data['PrevSaleDocumentType']) && in_array($data['PrevSaleDocumentType'], ['DT']))) {
            $isTrust = true;
            $trustDate = $data['PrevSaleRecordingDate'] ?? null;
        } elseif (isset($data['OwnerNAME1FULL']) && stripos($data['OwnerNAME1FULL'], 'TRUST') !== false) {
            $isTrust = true;
        }
        if ($isTrust) {
            $isRecent = true;
            if ($trustDate) {
                $trustDt = new DateTime($trustDate);
                $yearsAgo = $currentDate->diff($trustDt)->y;
                $isRecent = $yearsAgo <= 5;
            }
            if ($isRecent) {
                $tags[] = [
                    'category' => 'Ownership',
                    'flag' => 'Trust-Owned',
                    'priority' => 3,
                    'level' => 1,
                    'description' => 'Trust-owned properties may be part of estate planning',
                    'owner_name' => $data['OwnerNAME1FULL'] ?? null,
                    'current_doc_type' => $data['CurrentSaleDocumentType'] ?? null,
                    'prev_doc_type' => $data['PrevSaleDocumentType'] ?? null
                ];
            }
        }
        
        // Corporate Owner (Priority 4) - No time
        if ((isset($data['Owner1CorpInd']) && $data['Owner1CorpInd'] == 'T') ||
            (isset($data['Owner2CorpInd']) && $data['Owner2CorpInd'] == 'T') ||
            (isset($data['OwnerNAME1FULL']) && preg_match('/\b(LLC|INC|CORP|TRUST)\b/i', $data['OwnerNAME1FULL'])) ||
            (isset($data['OwnerNAME2FULL']) && preg_match('/\b(LLC|INC|CORP|TRUST)\b/i', $data['OwnerNAME2FULL']))) {
            $tags[] = [
                'category' => 'Ownership',
                'flag' => 'Corporate Owner',
                'priority' => 4,
                'level' => 1,
                'description' => 'Corporate owners may flip properties or negotiate wholesales',
                'owner1_corp' => $data['Owner1CorpInd'] ?? null,
                'owner2_corp' => $data['Owner2CorpInd'] ?? null,
                'owner1_name' => $data['OwnerNAME1FULL'] ?? null,
                'owner2_name' => $data['OwnerNAME2FULL'] ?? null
            ];
        }
        
        // Elderly Owner (Priority 5) - Level 2, with ownership >10 years
        $isElderly = false;
        if ((isset($data['SeniorInd']) && $data['SeniorInd'] == 'Y') ||
            (isset($data['Owner1Suffix']) && stripos($data['Owner1Suffix'], 'SR') !== false) ||
            (isset($data['Owner2Suffix']) && stripos($data['Owner2Suffix'], 'SR') !== false) ||
            (isset($data['HomesteadInd']) && $data['HomesteadInd'] == 'Y')) {
            $isElderly = true;
        }
        if ($isElderly) {
            $saleDateStr = $data['CurrentSaleRecordingDate'] ?? null;
            $isLong = false;
            if ($saleDateStr) {
                $saleDate = new DateTime($saleDateStr);
                $yearsOwned = $currentDate->diff($saleDate)->y;
                $isLong = $yearsOwned >= 10;
            }
            if ($isLong || !isset($saleDateStr)) {  // If long or no date (assume long)
                $tags[] = [
                    'category' => 'Ownership',
                    'flag' => 'Elderly Owner',
                    'priority' => 5,
                    'level' => 2,
                    'description' => 'Elderly owners may downsize or have estate sales',
                    'senior_ind' => $data['SeniorInd'] ?? null,
                    'owner1_suffix' => $data['Owner1Suffix'] ?? null,
                    'owner2_suffix' => $data['Owner2Suffix'] ?? null,
                    'homestead_ind' => $data['HomesteadInd'] ?? null
                ];
            }
        }
        
        // Owner-Occupied (Priority 6) - No time
        if (isset($data['OwnerOccupied']) && $data['OwnerOccupied'] == 'Y') {
            $tags[] = [
                'category' => 'Ownership',
                'flag' => 'Owner-Occupied',
                'priority' => 6,
                'level' => 1,
                'description' => 'Owner-occupiers may be less motivated but could indicate stable rentals',
                'owner_occupied' => $data['OwnerOccupied']
            ];
        }
        
        return $tags;
    }
    
    /**
     * Property Condition and Value-Add Flags
     */
    private static function getConditionTags($data, $currentDate)
    {
        $tags = [];
        
        // Fixer-Upper (Priority 1) - If condition poor or old build with no recent update (effective year within 10 years)
        $isFixer = false;
        if (isset($data['BuildingConditionCode']) && in_array($data['BuildingConditionCode'], [4, 5])) {
            $isFixer = true;
        } elseif (isset($data['YearBuilt']) && $data['YearBuilt'] < 1975) {
            $effYear = $data['EffectiveYearBuilt'] ?? $data['YearBuilt'];
            $effDt = new DateTime((string)$effYear . '-01-01');  // Approximate
            $yearsSinceUpdate = $currentDate->diff($effDt)->y;
            if ($yearsSinceUpdate >= 10) {
                $isFixer = true;
            }
        }
        if ($isFixer) {
            $tags[] = [
                'category' => 'Condition',
                'flag' => 'Fixer-Upper',
                'priority' => 1,
                'level' => 1,
                'description' => 'Needing repairs = high ROI flips',
                'building_condition' => $data['BuildingConditionCode'] ?? null,
                'year_built' => $data['YearBuilt'] ?? null,
                'effective_year_built' => $data['EffectiveYearBuilt'] ?? null
            ];
        }
        
        // Value-Add (Under-Improved) (Priority 2) - No time, structural
        $assdImprovementValue = $data['AssdImprovementValue'] ?? 0;
        $assdTotalValue = $data['AssdTotalValue'] ?? 0;
        $lotSizeAcres = $data['LotSizeAcres_int'] ?? 0;
        $buildingSqFt = $data['SumBuildingSqFt'] ?? 0;
        
        if ($assdTotalValue > 0 && $assdImprovementValue >= 0) {
            $improvementRatio = $assdImprovementValue / $assdTotalValue;
            if ($improvementRatio < 0.5 || ($lotSizeAcres > 0.5 && $buildingSqFt < 2000)) {
                $tags[] = [
                    'category' => 'Condition',
                    'flag' => 'Value-Add (Under-Improved)',
                    'priority' => 2,
                    'level' => 2,
                    'description' => 'Room for improvements/additions',
                    'improvement_ratio' => round($improvementRatio, 2),
                    'lot_size_acres' => $lotSizeAcres,
                    'building_sqft' => $buildingSqFt
                ];
            }
        }
        
        // Aging Property (Priority 3) - No time
        $currentYear = (int)$currentDate->format('Y');
        if (isset($data['YearBuilt']) && $data['YearBuilt'] < ($currentYear - 50)) {
            $tags[] = [
                'category' => 'Condition',
                'flag' => 'Aging Property',
                'priority' => 3,
                'level' => 1,
                'description' => 'Older homes may need updates',
                'year_built' => $data['YearBuilt'],
                'age_years' => $currentYear - $data['YearBuilt']
            ];
        }
        
        // Large Lot (Priority 4) - No time
        if ((isset($data['LotSizeAcres_int']) && $data['LotSizeAcres_int'] > 0.5) ||
            (isset($data['LotSizeSqFt']) && $data['LotSizeSqFt'] > 20000)) {
            $tags[] = [
                'category' => 'Condition',
                'flag' => 'Large Lot',
                'priority' => 4,
                'level' => 1,
                'description' => 'Subdivision or expansion potential',
                'lot_size_acres' => $data['LotSizeAcres_int'] ?? null,
                'lot_size_sqft' => $data['LotSizeSqFt'] ?? null
            ];
        }
        
        // Multi-Unit/Rental Potential (Priority 5) - No time
        if ((isset($data['SumResidentialUnits']) && $data['SumResidentialUnits'] > 1) ||
            (isset($data['PropertyClassID']) && $data['PropertyClassID'] == 'R' && 
             isset($data['SumBuildingsNbr']) && $data['SumBuildingsNbr'] > 1)) {
            $tags[] = [
                'category' => 'Condition',
                'flag' => 'Multi-Unit/Rental Potential',
                'priority' => 5,
                'level' => 1,
                'description' => 'Income-producing properties',
                'residential_units' => $data['SumResidentialUnits'] ?? null,
                'buildings_count' => $data['SumBuildingsNbr'] ?? null,
                'property_class' => $data['PropertyClassID'] ?? null
            ];
        }
        
        // Commercial Potential (Priority 6) - No time
        if (isset($data['PropertyClassID']) && in_array($data['PropertyClassID'], ['C', 'I'])) {
            $tags[] = [
                'category' => 'Condition',
                'flag' => 'Commercial Potential',
                'priority' => 6,
                'level' => 1,
                'description' => 'Conversion opportunities',
                'property_class' => $data['PropertyClassID'],
                'zoning' => $data['Zoning'] ?? null
            ];
        }
        
        // Pool Present (Priority 7) - No time
        if (isset($data['PoolCode']) && $data['PoolCode'] > 0) {
            $tags[] = [
                'category' => 'Condition',
                'flag' => 'Pool Present',
                'priority' => 7,
                'level' => 1,
                'description' => 'Pools add value but maintenance',
                'pool_code' => $data['PoolCode']
            ];
        }
        
        return $tags;
    }
    
    /**
     * Risk and Environmental Flags
     */
    private static function getRiskTags($data, $currentDate)
    {
        $tags = [];
        
        // Flood Zone (Priority 1) - Check map date for recency (within 5 years, else uncertain)
        if ((isset($data['flInsideSFHA']) && $data['flInsideSFHA'] == 'Y') ||
            (isset($data['flFemaFloodZone']) && preg_match('/[AEV]/', $data['flFemaFloodZone']))) {
            $isCurrent = true;
            $mapDateStr = $data['flFemaMapDate'] ?? null;
            if ($mapDateStr) {
                $mapDate = new DateTime($mapDateStr);
                $yearsOld = $currentDate->diff($mapDate)->y;
                $isCurrent = $yearsOld <= 5;
            }
            $description = $isCurrent ? 'Insurance costs impact value' : 'Flood zone (check updated maps)';
            $tags[] = [
                'category' => 'Risk',
                'flag' => 'Flood Zone',
                'priority' => 1,
                'level' => 1,
                'description' => $description,
                'inside_sfha' => $data['flInsideSFHA'] ?? null,
                'fema_zone' => $data['flFemaFloodZone'] ?? null
            ];
        }
        
        // HOA Property (Priority 2) - No time
        if ((isset($data['HOA1Name']) && !empty($data['HOA1Name'])) ||
            (isset($data['HOA2Name']) && !empty($data['HOA2Name']))) {
            $tags[] = [
                'category' => 'Risk',
                'flag' => 'HOA Property',
                'priority' => 2,
                'level' => 1,
                'description' => 'HOA fees affect buyers',
                'hoa1_name' => $data['HOA1Name'] ?? null,
                'hoa1_fee' => $data['HOA1FeeValue'] ?? null,
                'hoa2_name' => $data['HOA2Name'] ?? null
            ];
        }
        
        // Historical Property (Priority 3) - No time
        if ((isset($data['SiteInfluenceCode']) && $data['SiteInfluenceCode'] == '8') ||
            (isset($data['YearBuilt']) && $data['YearBuilt'] < 1900)) {
            $tags[] = [
                'category' => 'Risk',
                'flag' => 'Historical Property',
                'priority' => 3,
                'level' => 1,
                'description' => 'Tax credits but renovation restrictions',
                'site_influence' => $data['SiteInfluenceCode'] ?? null,
                'year_built' => $data['YearBuilt'] ?? null
            ];
        }
        
        // Mobile Home (Priority 4) - No time
        if (isset($data['MobileHomeInd']) && $data['MobileHomeInd'] == 'Y') {
            $tags[] = [
                'category' => 'Risk',
                'flag' => 'Mobile Home',
                'priority' => 4,
                'level' => 1,
                'description' => 'Cheaper entry, niche plays',
                'mobile_home_ind' => $data['MobileHomeInd']
            ];
        }
        
        // Basement Present (Priority 5) - No time
        if ((isset($data['BasementCode']) && $data['BasementCode'] != 5) ||
            (isset($data['SumBasementSqFt']) && $data['SumBasementSqFt'] > 0)) {
            $tags[] = [
                'category' => 'Risk',
                'flag' => 'Basement Present',
                'priority' => 5,
                'level' => 1,
                'description' => 'Adds living space',
                'basement_code' => $data['BasementCode'] ?? null,
                'basement_sqft' => $data['SumBasementSqFt'] ?? null
            ];
        }
        
        // View/Amenities Premium (Priority 6) - No time
        if (isset($data['Amenities']) && !empty($data['Amenities']) && 
            preg_match('/\b(Golf|Lake|Ocean|Mountain|View)\b/i', $data['Amenities'])) {
            $tags[] = [
                'category' => 'Risk',
                'flag' => 'View/Amenities Premium',
                'priority' => 6,
                'level' => 1,
                'description' => 'Higher resale value',
                'amenities' => $data['Amenities'],
                'site_influence' => $data['SiteInfluenceCode'] ?? null
            ];
        }
        
        return $tags;
    }
    
    /**
     * Vacancy and Distress Flags
     */
    private static function getDistressTags($data, $currentDate)
    {
        $tags = [];
        
        // Foreclosure/REO (Priority 1) - Within 24 months
        $isForeclosure = false;
        $fcDate = null;
        if (isset($data['CurrentSaleDocumentType']) && in_array($data['CurrentSaleDocumentType'], ['34', '64', 'N'])) {
            $isForeclosure = true;
            $fcDate = $data['CurrentSaleRecordingDate'] ?? null;
        } elseif (isset($data['PrevSaleDocumentType']) && in_array($data['PrevSaleDocumentType'], ['34', '64', 'N'])) {
            $isForeclosure = true;
            $fcDate = $data['PrevSaleRecordingDate'] ?? null;
        } elseif (isset($data['CurrentSaleSeller1FullName']) && stripos($data['CurrentSaleSeller1FullName'], 'BANK') !== false) {
            $isForeclosure = true;
            $fcDate = $data['CurrentSaleRecordingDate'] ?? null;
        }
        if ($isForeclosure && $fcDate) {
            $fcDt = new DateTime($fcDate);
            $monthsAgo = ($currentDate->diff($fcDt)->y * 12) + $currentDate->diff($fcDt)->m;
            if ($monthsAgo <= 24) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Foreclosure/REO',
                    'priority' => 1,
                    'level' => 1,
                    'description' => 'Bank-owned or REO properties sell at discounts',
                    'current_doc_type' => $data['CurrentSaleDocumentType'] ?? null,
                    'prev_doc_type' => $data['PrevSaleDocumentType'] ?? null,
                    'seller_name' => $data['CurrentSaleSeller1FullName'] ?? null
                ];
            }
        }
        
        // Pre-Foreclosure (Priority 2) - Within 12 months
        $isPreFc = false;
        $preFcDate = null;
        if (isset($data['CurrentSaleDocumentType']) && in_array($data['CurrentSaleDocumentType'], ['77', '78', '79', '81'])) {
            $isPreFc = true;
            $preFcDate = $data['CurrentSaleRecordingDate'] ?? null;
        } elseif (isset($data['PrevSaleDocumentType']) && in_array($data['PrevSaleDocumentType'], ['77', '78', '79', '81'])) {
            $isPreFc = true;
            $preFcDate = $data['PrevSaleRecordingDate'] ?? null;
        }
        if ($isPreFc && $preFcDate) {
            $preFcDt = new DateTime($preFcDate);
            $monthsAgo = ($currentDate->diff($preFcDt)->y * 12) + $currentDate->diff($preFcDt)->m;
            if ($monthsAgo <= 12) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Pre-Foreclosure',
                    'priority' => 2,
                    'level' => 1,
                    'description' => 'Early distress signal for short sales',
                    'current_doc_type' => $data['CurrentSaleDocumentType'] ?? null,
                    'prev_doc_type' => $data['PrevSaleDocumentType'] ?? null
                ];
            }
        }
        
        // Probate (Priority 3) - Within 24 months
        $isProbate = false;
        $probateDate = null;
        if (isset($data['CurrentSaleDocumentType']) && in_array($data['CurrentSaleDocumentType'], ['33', '19', '13'])) {
            $isProbate = true;
            $probateDate = $data['CurrentSaleRecordingDate'] ?? null;
        } elseif (isset($data['PrevSaleDocumentType']) && in_array($data['PrevSaleDocumentType'], ['33', '19', '13'])) {
            $isProbate = true;
            $probateDate = $data['PrevSaleRecordingDate'] ?? null;
        }
        if ($isProbate && $probateDate) {
            $probateDt = new DateTime($probateDate);
            $monthsAgo = ($currentDate->diff($probateDt)->y * 12) + $currentDate->diff($probateDt)->m;
            if ($monthsAgo <= 24) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Probate',
                    'priority' => 3,
                    'level' => 1,
                    'description' => 'Estate sales often undervalue properties',
                    'current_doc_type' => $data['CurrentSaleDocumentType'] ?? null,
                    'prev_doc_type' => $data['PrevSaleDocumentType'] ?? null
                ];
            }
        }
        
        // Pre-Probate (Priority 4) - Within 12 months
        $isPreProbate = false;
        $preProbateDate = null;
        if (isset($data['CurrentSaleDocumentType']) && in_array($data['CurrentSaleDocumentType'], ['29', 'M'])) {
            $isPreProbate = true;
            $preProbateDate = $data['CurrentSaleRecordingDate'] ?? null;
        } elseif (isset($data['PrevSaleDocumentType']) && in_array($data['PrevSaleDocumentType'], ['29', 'M'])) {
            $isPreProbate = true;
            $preProbateDate = $data['PrevSaleRecordingDate'] ?? null;
        }
        if ($isPreProbate && $preProbateDate) {
            $preProbateDt = new DateTime($preProbateDate);
            $monthsAgo = ($currentDate->diff($preProbateDt)->y * 12) + $currentDate->diff($preProbateDt)->m;
            if ($monthsAgo <= 12) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Pre-Probate',
                    'priority' => 4,
                    'level' => 1,
                    'description' => 'Owner death signals upcoming sales',
                    'current_doc_type' => $data['CurrentSaleDocumentType'] ?? null,
                    'prev_doc_type' => $data['PrevSaleDocumentType'] ?? null
                ];
            }
        }
        
        // Tax Delinquent (Priority 5) - Current or last year
        if (isset($data['TaxDeliquentYear']) && !empty($data['TaxDeliquentYear'])) {
            $delYear = (int)$data['TaxDeliquentYear'];
            $curYear = (int)$currentDate->format('Y');
            if ($delYear >= $curYear - 1) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Tax Delinquent',
                    'priority' => 5,
                    'level' => 1,
                    'description' => 'Delinquent taxes indicate financial distress',
                    'delinquent_year' => $data['TaxDeliquentYear'] ?? null,
                    'tax_year' => $data['TaxYear'] ?? null,
                    'tax_amount' => $data['TaxAmt'] ?? null
                ];
            }
        } elseif (isset($data['TaxYear']) && isset($data['TaxAmt']) && 
             ((int)$data['TaxYear'] < (int)$currentDate->format('Y') - 1) && $data['TaxAmt'] > 1000) {
            $tags[] = [
                'category' => 'Distress',
                'flag' => 'Tax Delinquent',
                'priority' => 5,
                'level' => 1,
                'description' => 'Delinquent taxes indicate financial distress',
                'delinquent_year' => $data['TaxDeliquentYear'] ?? null,
                'tax_year' => $data['TaxYear'] ?? null,
                'tax_amount' => $data['TaxAmt'] ?? null
            ];
        }
        
        // Vacant (Priority 6) - Within 12 months
        if (isset($data['VacantFlag']) && $data['VacantFlag'] == 'Y') {
            $vacDateStr = $data['VacantFlagDate'] ?? null;
            $isRecent = true;
            if ($vacDateStr) {
                $vacDate = new DateTime($vacDateStr);
                $monthsAgo = ($currentDate->diff($vacDate)->y * 12) + $currentDate->diff($vacDate)->m;
                $isRecent = $monthsAgo <= 12;
            }
            if ($isRecent) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Vacant',
                    'priority' => 6,
                    'level' => 1,
                    'description' => 'Vacant properties are prone to vandalism and motivated sellers',
                    'vacant_flag' => $data['VacantFlag'] ?? null,
                    'vacant_date' => $data['VacantFlagDate'] ?? null
                ];
            }
        }
        
        // Special Document Type (Priority 8) - Within 24 months for distress docs
        if (isset($data['CurrentSaleDocumentType']) && !empty($data['CurrentSaleDocumentType'])) {
            $docType = $data['CurrentSaleDocumentType'];
            $docDate = $data['CurrentSaleRecordingDate'] ?? null;
            if (in_array($docType, ['27', '28', '29', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50'])) {
                $isRecent = true;
                if ($docDate) {
                    $docDt = new DateTime($docDate);
                    $monthsAgo = ($currentDate->diff($docDt)->y * 12) + $currentDate->diff($docDt)->m;
                    $isRecent = $monthsAgo <= 24;
                }
                if ($isRecent) {
                    $tags[] = [
                        'category' => 'Distress',
                        'flag' => 'Special Document Type',
                        'priority' => 8,
                        'level' => 1,
                        'description' => 'Special document type may indicate distress or unique circumstances',
                        'doc_type' => $docType
                    ];
                }
            }
        }
        
        // Low Confidence Value (Priority 8) - If valuation recent
        if (isset($data['vConfidenceScore']) && $data['vConfidenceScore'] > 0 && $data['vConfidenceScore'] < 50) {
            $valDateStr = $data['vValuationDate'] ?? null;
            $isRecent = true;
            if ($valDateStr) {
                $valDate = new DateTime($valDateStr);
                $monthsAgo = ($currentDate->diff($valDate)->y * 12) + $currentDate->diff($valDate)->m;
                $isRecent = $monthsAgo <= 12;
            }
            if ($isRecent) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Low Confidence Value',
                    'priority' => 8,
                    'level' => 2,
                    'description' => 'Low AVM confidence may indicate distress or unique property',
                    'confidence_score' => $data['vConfidenceScore']
                ];
            }
        }
        
        // Long-term Ownership (Priority 8) - >20 years (current status)
        if (isset($data['CurrentSaleRecordingDate']) && !empty($data['CurrentSaleRecordingDate'])) {
            $saleDate = new DateTime($data['CurrentSaleRecordingDate']);
            $yearsOwned = $currentDate->diff($saleDate)->y;
            if ($yearsOwned > 20) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Long-term Ownership',
                    'priority' => 8,
                    'level' => 2,
                    'description' => 'Long ownership may indicate estate planning or distress',
                    'sale_date' => $data['CurrentSaleRecordingDate'],
                    'ownership_years' => $yearsOwned
                ];
            }
        }
        
        // Exemptions/Indigency (Priority 9) - No time, assumed current
        if ((isset($data['WelfareInd']) && $data['WelfareInd'] == 'Y') ||
            (isset($data['DisabledInd']) && $data['DisabledInd'] == 'Y') ||
            (isset($data['WidowInd']) && $data['WidowInd'] == 'Y') ||
            (isset($data['VeteranInd']) && $data['VeteranInd'] == 'Y')) {
            $tags[] = [
                'category' => 'Distress',
                'flag' => 'Exemptions/Indigency',
                'priority' => 9,
                'level' => 1,
                'description' => 'Exemptions may indicate hardships',
                'welfare_ind' => $data['WelfareInd'] ?? null,
                'disabled_ind' => $data['DisabledInd'] ?? null,
                'widow_ind' => $data['WidowInd'] ?? null,
                'veteran_ind' => $data['VeteranInd'] ?? null
            ];
        }
        
        // Potential Tax Issues (Priority 8) - Tax year < current -2 (current indicator)
        if (isset($data['TaxAmt']) && $data['TaxAmt'] > 0) {
            $taxYear = (int)($data['TaxYear'] ?? 0);
            $curYear = (int)$currentDate->format('Y');
            if ($taxYear < $curYear - 2) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Potential Tax Issues',
                    'priority' => 8,
                    'level' => 2,
                    'description' => 'Older tax year may indicate payment issues',
                    'tax_year' => $taxYear,
                    'tax_amount' => $data['TaxAmt']
                ];
            }
        }
        
        // Low Market Value (Priority 8) - Current ratio
        $marketValue = $data['MarketTotalValue'] ?? 0;
        $assessedValue = $data['AssdTotalValue'] ?? 0;
        if ($marketValue > 0 && $assessedValue > 0) {
            $valueRatio = $marketValue / $assessedValue;
            if ($valueRatio < 0.7) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'Low Market Value',
                    'priority' => 8,
                    'level' => 2,
                    'description' => 'Market value significantly below assessed value',
                    'market_value' => $marketValue,
                    'assessed_value' => $assessedValue,
                    'value_ratio' => round($valueRatio, 2)
                ];
            }
        }
        
        // No Recent Activity (Priority 8) - Last sale >10 years (current stagnation)
        if (isset($data['CurrentSaleRecordingDate']) && !empty($data['CurrentSaleRecordingDate'])) {
            $saleDate = new DateTime($data['CurrentSaleRecordingDate']);
            $yearsAgo = $currentDate->diff($saleDate)->y;
            if ($yearsAgo > 10) {
                $tags[] = [
                    'category' => 'Distress',
                    'flag' => 'No Recent Activity',
                    'priority' => 8,
                    'level' => 2,
                    'description' => 'No recent sales activity may indicate distress',
                    'last_sale_date' => $data['CurrentSaleRecordingDate'],
                    'years_since_sale' => $yearsAgo
                ];
            }
        }
        
        return $tags;
    }
    
    /**
     * Helper function to calculate remaining mortgage balance
     */
    private static function calculateRemainingBalance($principal, $annualRate, $termYears, $yearsPassed)
    {
        $monthlyRate = $annualRate / 12 / 100;
        $termMonths = $termYears * 12;
        $monthsPassed = $yearsPassed * 12;
        if ($monthlyRate == 0) {
            return max(0, $principal * (1 - $monthsPassed / $termMonths));
        }
        $monthlyPayment = $principal * ($monthlyRate * pow(1 + $monthlyRate, $termMonths)) / (pow(1 + $monthlyRate, $termMonths) - 1);
        $balance = $principal * pow(1 + $monthlyRate, $monthsPassed) - $monthlyPayment * (pow(1 + $monthlyRate, $monthsPassed) - 1) / $monthlyRate;
        return max(0, $balance);
    }
    
    /**
     * Helper function to check if property has long-term ownership
     */
    private static function isLongTermOwnership($saleDate, $currentDate, $thresholdYears = 20)
    {
        if (empty($saleDate)) return false;
        
        $sale = new DateTime($saleDate);
        $years = $currentDate->diff($sale)->y;
        
        return $years >= $thresholdYears;
    }
}