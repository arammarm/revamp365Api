<?php
ini_set('memory_limit', '4G');
set_time_limit(180);

include_once "mysql_conn.php";
include_once "../includes/APIHelper.php";

APIHelper::Authentication();

global $dsn, $username, $password;
try {
    $conn = new PDO($dsn, $username, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // --- User Parameters ---
    $reqAddress = APIHelper::GetParam('address', true);
    $reqZip = APIHelper::GetParam('zip', true);
    $reqLat = APIHelper::GetParam('lat', true);
    $reqLng = APIHelper::GetParam('lng', true);
    $reqRange = APIHelper::GetParam('range', true);
    $reqDays = APIHelper::GetParam('days', true);
    $reqFips = APIHelper::GetParam('fips', true);

    $zipCode = $reqZip;
    if (empty($zipCode)) {
        preg_match_all('/\b\d{5}\b/', $reqAddress, $matches);
        if (!empty($matches[0])) {
            $zipCode = end($matches[0]);
            if (!ctype_digit($zipCode) || strlen($zipCode) !== 5) {
                APIHelper::SendResponse(null, 0, 'Invalid ZIP Code found in address.');
                die();
            }
        } else {
            APIHelper::SendResponse(null, 0, 'No ZIP Code found in address.');
            die();
        }
    }

    // Pre-calculate bounding box values
    $lat_min = $reqLat - ($reqRange / 69.0);
    $lat_max = $reqLat + ($reqRange / 69.0);
    $lng_min = $reqLng - ($reqRange / (69.0 * cos(deg2rad($reqLat))));
    $lng_max = $reqLng + ($reqRange / (69.0 * cos(deg2rad($reqLat))));

    // --- STEP 1A: Get locally active BUYERS ---
    $sql_local_buyers = "
        SELECT
            UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) AS mailing_key,
            COUNT(*) AS local_purchase_count
        FROM datatree_property p
        WHERE
            p.FIPS = :fips
            AND p.CurrentSaleRecordingDate IS NOT NULL
            AND STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            AND (p.SitusLatitude BETWEEN :lat_min AND :lat_max)
            AND (p.SitusLongitude BETWEEN :lng_min AND :lng_max)
            AND 3959 * ACOS(
                COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
                COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
                SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
            ) <= :radius_miles
        GROUP BY mailing_key
        ORDER BY local_purchase_count DESC
        LIMIT 300;
    ";

    $stmt_buyers = $conn->prepare($sql_local_buyers);
    $stmt_buyers->bindParam(":fips", $reqFips);
    $stmt_buyers->bindParam(":days", $reqDays);
    $stmt_buyers->bindParam(":center_lat", $reqLat);
    $stmt_buyers->bindParam(":center_lng", $reqLng);
    $stmt_buyers->bindParam(":radius_miles", $reqRange);
    $stmt_buyers->bindParam(":lat_min", $lat_min);
    $stmt_buyers->bindParam(":lat_max", $lat_max);
    $stmt_buyers->bindParam(":lng_min", $lng_min);
    $stmt_buyers->bindParam(":lng_max", $lng_max);
    $stmt_buyers->execute();

    $buyer_results = $stmt_buyers->fetchAll(PDO::FETCH_ASSOC);
    $buyer_mailing_keys = array_column($buyer_results, 'mailing_key');

    // --- STEP 1B: Get recent SELLERS and find their mailing addresses ---
    $sql_recent_sellers = "
        SELECT DISTINCT
            UPPER(TRIM(p.CurrentSaleSeller1FullName)) AS seller_name
        FROM datatree_property p
        WHERE
            p.FIPS = :fips
            AND p.CurrentSaleRecordingDate IS NOT NULL
            AND STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            AND (p.SitusLatitude BETWEEN :lat_min AND :lat_max)
            AND (p.SitusLongitude BETWEEN :lng_min AND :lng_max)
            AND 3959 * ACOS(
                COS(RADIANS(:center_lat)) * COS(RADIANS(p.SitusLatitude)) *
                COS(RADIANS(p.SitusLongitude) - RADIANS(:center_lng)) +
                SIN(RADIANS(:center_lat)) * SIN(RADIANS(p.SitusLatitude))
            ) <= :radius_miles
            AND p.CurrentSaleSeller1FullName IS NOT NULL
            AND p.CurrentSaleSeller1FullName != ''
            -- Focus on likely investor sellers
            AND (UPPER(p.CurrentSaleSeller1FullName) LIKE '%LLC%' 
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%INC%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%CORP%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%TRUST%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%PROPERTIES%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%INVESTMENTS%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%HOMES%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%REALTY%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%CAPITAL%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%VENTURES%'
                 OR UPPER(p.CurrentSaleSeller1FullName) LIKE '%GROUP%')
        LIMIT 200;
    ";

    $stmt_sellers = $conn->prepare($sql_recent_sellers);
    $stmt_sellers->bindParam(":fips", $reqFips);
    $stmt_sellers->bindParam(":days", $reqDays);
    $stmt_sellers->bindParam(":center_lat", $reqLat);
    $stmt_sellers->bindParam(":center_lng", $reqLng);
    $stmt_sellers->bindParam(":radius_miles", $reqRange);
    $stmt_sellers->bindParam(":lat_min", $lat_min);
    $stmt_sellers->bindParam(":lat_max", $lat_max);
    $stmt_sellers->bindParam(":lng_min", $lng_min);
    $stmt_sellers->bindParam(":lng_max", $lng_max);
    $stmt_sellers->execute();

    $seller_names = $stmt_sellers->fetchAll(PDO::FETCH_COLUMN, 0);
    $seller_mailing_keys = [];

    // --- STEP 1C: Find mailing addresses for the sellers ---
    if (!empty($seller_names)) {
        $seller_placeholders = implode(',', array_fill(0, count($seller_names), '?'));
        $sql_seller_mailing = "
            SELECT DISTINCT
                UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) AS mailing_key
            FROM datatree_property p
            WHERE UPPER(TRIM(p.OwnerNAME1FULL)) IN ($seller_placeholders)
                AND MailingFullStreetAddress IS NOT NULL
                AND MailingFullStreetAddress != ''
            LIMIT 200;
        ";

        $stmt_seller_mailing = $conn->prepare($sql_seller_mailing);
        $stmt_seller_mailing->execute($seller_names);
        $seller_mailing_keys = $stmt_seller_mailing->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    // --- STEP 1D: Combine buyer and seller mailing keys ---
    $all_mailing_keys = array_unique(array_merge($buyer_mailing_keys, $seller_mailing_keys));
    
    if (empty($all_mailing_keys)) {
        APIHelper::SendResponse([], 1, 'No local investors found matching criteria.');
        die();
    }
    
    // --- STEP 2: Get Nationwide & Local stats for ALL mailing keys (buyers + sellers) ---
    $placeholders = implode(',', array_fill(0, count($all_mailing_keys), '?'));
    $sql_stats = "
        SELECT
            UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) AS investor_identifier,
            
            -- Fields for final output
            GROUP_CONCAT(DISTINCT OwnerNAME1FULL SEPARATOR ', ') as investor_alias,
            MAX(MailingFullStreetAddress) as MailingFullStreetAddress,
            MAX(MailingCity) as MailingCity,
            MAX(MailingState) as MailingState,
            MAX(MailingZIP5) as MailingZIP5,

            -- Nationwide Stats
            COUNT(PropertyID) AS total_properties_owned_nationwide,
            SUM(CASE WHEN OwnerOccupied = 'N' OR SitusState != MailingState THEN 1 ELSE 0 END) AS total_absentee_properties,
            SUM(CASE WHEN ConcurrentMtg1LoanAmt IS NULL OR ConcurrentMtg1LoanAmt = 0 THEN 1 ELSE 0 END) AS total_cash_purchases,
            MAX(CASE WHEN Owner1CorpInd IN ('Y','T','1') OR UPPER(OwnerNAME1FULL) LIKE '%LLC%' THEN 1 ELSE 0 END) AS is_llc_nationwide,

            -- Local Purchase Stats
            SUM(CASE 
                WHEN FIPS = ? AND STR_TO_DATE(CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                THEN 1 ELSE 0 END
            ) AS local_properties_owned,
            SUM(CASE WHEN SitusZIP5 = ? THEN 1 ELSE 0 END) AS properties_in_target_zip,
            MAX(CASE 
                WHEN FIPS = ? AND STR_TO_DATE(CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                THEN CurrentSaleRecordingDate ELSE NULL END
            ) AS most_recent_local_purchase

        FROM datatree_property
        WHERE UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) IN ($placeholders)
        GROUP BY investor_identifier
    ";

    $stmt_stats = $conn->prepare($sql_stats);
    $params = array_merge([$reqFips, $reqDays, $zipCode, $reqFips, $reqDays], $all_mailing_keys);
    $stmt_stats->execute($params);
    $investor_stats = $stmt_stats->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

    // --- STEP 2B: Get local sales data separately ---
    $sales_lookup = [];
    if (!empty($seller_names)) {
        $seller_placeholders = implode(',', array_fill(0, count($seller_names), '?'));
        $sql_sales_stats = "
            SELECT
                s.seller_name,
                COUNT(*) as local_sales_count,
                MAX(p.CurrentSaleRecordingDate) as most_recent_sale_date
            FROM (
                SELECT UPPER(TRIM(CurrentSaleSeller1FullName)) as seller_name
                FROM datatree_property
                WHERE FIPS = ?
                    AND CurrentSaleRecordingDate IS NOT NULL
                    AND STR_TO_DATE(CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                    AND UPPER(TRIM(CurrentSaleSeller1FullName)) IN ($seller_placeholders)
            ) s
            JOIN datatree_property p ON UPPER(TRIM(p.CurrentSaleSeller1FullName)) = s.seller_name
            WHERE p.FIPS = ?
                AND STR_TO_DATE(p.CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
            GROUP BY s.seller_name
        ";

        $stmt_sales = $conn->prepare($sql_sales_stats);
        $sales_params = array_merge([$reqFips, $reqDays], $seller_names, [$reqFips, $reqDays]);
        $stmt_sales->execute($sales_params);
        $sales_stats = $stmt_sales->fetchAll(PDO::FETCH_ASSOC);

        // Create a lookup array for sales data
        foreach ($sales_stats as $sale) {
            $sales_lookup[$sale['seller_name']] = $sale;
        }
    }

    // --- STEP 3: Get details of the single most recent LOCAL purchase for each mailing key ---
    $sql_mrp_details = "
        WITH RankedPurchases AS (
            SELECT
                UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) AS investor_identifier,
                OwnerNAME1FULL AS mrp_owner_name,
                SitusFullStreetAddress AS mrp_fullstreet, SitusCity AS mrp_city, SitusState AS mrp_state, SitusZIP5 AS mrp_zip,
                CurrentSalesPrice AS mrp_sales_price, Bedrooms AS mrp_beds, BathFull AS mrp_bath, SumLivingAreaSqFt AS mrp_sqft,
                BuildingClassCode AS mrp_type_class, SitusLatitude AS mrp_lat, SitusLongitude AS mrp_lng, FIPS,
                ROW_NUMBER() OVER(PARTITION BY UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) ORDER BY CurrentSaleRecordingDate DESC) as rn
            FROM datatree_property
            WHERE UPPER(TRIM(REGEXP_REPLACE(CONCAT(MailingFullStreetAddress, ' ', MailingCity, ' ', MailingState, ' ', MailingZIP5), '[^A-Z0-9 ]', ''))) IN ($placeholders)
              AND FIPS = ?
              AND STR_TO_DATE(CurrentSaleRecordingDate, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        )
        SELECT * FROM RankedPurchases WHERE rn = 1;
    ";
    $stmt_mrp = $conn->prepare($sql_mrp_details);
    $mrp_params = array_merge($all_mailing_keys, [$reqFips, $reqDays]);
    $stmt_mrp->execute($mrp_params);
    $mrp_details = $stmt_mrp->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

    // --- STEP 4: Combine all data in PHP and calculate the final score ---
    $final_results = [];
    $max_score = 0;
    foreach ($investor_stats as $identifier => $stats_array) {
        $stats = $stats_array[0]; 
        $details = isset($mrp_details[$identifier]) ? $mrp_details[$identifier][0] : [];

        // Check if this investor has recent sales activity
        $local_sales_count = 0;
        $most_recent_sale = null;
        if (!empty($seller_names)) {
            $investor_names = explode(', ', $stats['investor_alias']);
            foreach ($investor_names as $name) {
                $clean_name = strtoupper(trim($name));
                if (isset($sales_lookup[$clean_name])) {
                    $local_sales_count += $sales_lookup[$clean_name]['local_sales_count'];
                    if (!$most_recent_sale || $sales_lookup[$clean_name]['most_recent_sale_date'] > $most_recent_sale) {
                        $most_recent_sale = $sales_lookup[$clean_name]['most_recent_sale_date'];
                    }
                }
            }
        }

        // Calculate days since most recent activity (purchase OR sale)
        $most_recent_purchase = $stats['most_recent_local_purchase'];
        $most_recent_activity = null;
        if ($most_recent_purchase && $most_recent_sale) {
            $most_recent_activity = max($most_recent_purchase, $most_recent_sale);
        } elseif ($most_recent_purchase) {
            $most_recent_activity = $most_recent_purchase;
        } elseif ($most_recent_sale) {
            $most_recent_activity = $most_recent_sale;
        }
        
        $days_since = $most_recent_activity ? (new DateTime())->diff(new DateTime($most_recent_activity))->days : 9999;
        
        // Enhanced score including selling activity
        $score = 
            (min($stats['total_properties_owned_nationwide'], 20) * 5) +
            (min($stats['total_cash_purchases'], 50) * 2) +
            ($stats['is_llc_nationwide'] * 10) +
            (log1p($stats['total_absentee_properties']) * 5) +
            (min($stats['properties_in_target_zip'], 20) * 15) +
            ($local_sales_count * 20) +
            (30 * exp(-$days_since / 90));

        if ($score > $max_score) {
            $max_score = $score;
        }

        // --- NEW: Determine Investor Type Tags ---
        $investor_tags = [];
        
        // Flipper Logic
        $sales_to_ownership_ratio = $stats['total_properties_owned_nationwide'] > 0 ? 
            ($local_sales_count / $stats['total_properties_owned_nationwide']) : 0;
        
        $is_flipper = (
            $local_sales_count >= 3 && // At least 3 recent sales
            ($sales_to_ownership_ratio >= 0.5 || // High sales-to-ownership ratio OR
             ($local_sales_count >= $stats['local_properties_owned'] && $local_sales_count >= 5)) // More sales than current local holdings
        );
        
        // Landlord Logic  
        $is_landlord = (
            $stats['total_properties_owned_nationwide'] >= 8 && // Owns at least 8 properties
            $stats['total_absentee_properties'] >= 3 && // At least 3 absentee properties
            $local_sales_count <= 2 // Low recent sales activity
        );
        
        // Portfolio Builder (hybrid - buys locally but holds)
        $is_portfolio_builder = (
            $stats['properties_in_target_zip'] >= 3 && // Building local portfolio
            $stats['local_properties_owned'] >= 3 && // Recent local purchases
            $local_sales_count <= 1 // But not selling much
        );
        
        // Assign tags
        if ($is_flipper) {
            $investor_tags[] = 'Flipper';
        }
        if ($is_landlord) {
            $investor_tags[] = 'Landlord';
        }
        if ($is_portfolio_builder && !$is_flipper) {
            $investor_tags[] = 'Portfolio Builder';
        }
        
        // Default tag if no specific type identified
        if (empty($investor_tags)) {
            if ($stats['total_properties_owned_nationwide'] >= 5) {
                $investor_tags[] = 'Investor';
            } else {
                $investor_tags[] = 'Small Investor';
            }
        }

        $combined_data = array_merge($stats, $details);
        $combined_data['investor_name'] = $details['mrp_owner_name'] ?? explode(', ', $stats['investor_alias'])[0];
        $combined_data['days_since_last_local_activity'] = $days_since;
        $combined_data['local_properties_sold'] = $local_sales_count;
        $combined_data['most_recent_local_sale'] = $most_recent_sale;
        $combined_data['most_recent_activity_type'] = ($most_recent_activity == $most_recent_sale) ? 'SALE' : 'PURCHASE';
        $combined_data['likelihood_score'] = $score;
        $combined_data['investor_type_tags'] = implode(', ', $investor_tags); // NEW FIELD
        
        $final_results[] = $combined_data;
    }

    // Normalize scores to 0-100 if max_score > 0
    if ($max_score > 0) {
        foreach ($final_results as &$row) {
            $row['likelihood_score'] = round(($row['likelihood_score'] / $max_score) * 100);
        }
    }

    usort($final_results, function($a, $b) {
        return $b['likelihood_score'] <=> $a['likelihood_score'];
    });

    foreach ($final_results as $i => &$row) {
        $row['top_investor_tag'] = ($i < 5) ? 'Top Investor' : '';
    }

    APIHelper::SendResponse(array_slice($final_results, 0, 50), count($final_results));
    die();

} catch (PDOException $e) {
    APIHelper::SendResponse(null, 0, "Query failed: " . $e->getMessage());
    die();
}
