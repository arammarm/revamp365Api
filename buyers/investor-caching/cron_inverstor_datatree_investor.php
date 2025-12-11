<?php
// File: /opt/etl/investor_cache_incremental_etl.php
// Schedule: 0 2 * * 0 (Weekly, Sunday 2 AM)

include_once 'config.inverstercache.php';
include_once __DIR__ . '/../../includes/DatatreeAPIHelperExtended.php';

class IncrementalInvestorETL
{
    private $mysqlConn;
    private $pgConn;
    private $batchSize = 15000;  // Smaller batches for incremental
    private $logFile = '/var/www/html/cron/API/buyers/investor-caching/logs/etl.log';
    private $stateFile = '/var/www/html/cron/API/buyers/investor-caching/etl/investor_etl_state.json';
    private $startTime;

    private $logging = true;
    private $processId;
    private $batchId;

    public function __construct()
    {
        $this->startTime = microtime(true);
        $this->initConnections();
        $this->processId = $this->getProcessId();
        $this->batchId = $this->getBatchId();
    }
    private function getProcessId()
    {
        // Generate a unique process ID as string (for process_id column)
        return 'ETL_' . date('YmdHis') . '_' . rand(100, 999);
    }
    private function getBatchId()
    {
        // Generate a unique batch ID as integer (for batch_id column)
        return rand(100000, 999999);
    }
    private function initConnections()
    {
        // MySQL source (read-only replica preferred)
        $this->mysqlConn = new PDO(
            "mysql:host=" . MYSQL_HOST . ";dbname=" . MYSQL_DB . ";charset=utf8mb4",
            MYSQL_USER,
            MYSQL_PASS,
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false
            ]
        );

        // PostgreSQL destination
        $this->pgConn = new PDO(
            "pgsql:host=" . PG_HOST . ";port=" . PG_PORT . ";dbname=" . PG_DB,
            PG_USER,
            PG_PASS,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
    }

    /**
     * Get last successful ETL run timestamp
     */

    private function getLastRunState()
    {
        $defaultState = [
            'last_run_time' => date('Y-m-d', strtotime('-5 days')),
            'last_property_id' => 0,
            'total_processed' => 0
        ];
        if (!file_exists($this->stateFile)) {
            return $defaultState;
        }
        $jsonData = json_decode(file_get_contents($this->stateFile), true);
        if (!is_array($jsonData) || !isset($jsonData['last_run_time'])) {
            return $defaultState;
        }
        // Subtract 5 days from the saved last_run_time
        $jsonData['last_run_time'] = date('Y-m-d', strtotime($jsonData['last_run_time'] . ' -5 days'));
        return $jsonData;
    }

    /**
     * Save ETL state for next run
     */
    private function saveRunState($state)
    {
        $dir = dirname($this->stateFile);
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
        file_put_contents($this->stateFile, json_encode($state));
    }


    public function run()
    {
        try {
            $this->startTime = microtime(true);

            $this->log("=== Incremental ETL Started ===");
            $state = $this->getLastRunState();

            // Only for initial run to process all data 
           //$state['last_run_time'] = '2025-12-05'; 

            $this->log("Last run: {$state['last_run_time']}");
            // Track metrics
            $metrics = [
                'investors_updated' => 0,
                'investors_new' => 0,
                'properties_updated' => 0,
                'properties_new' => 0
            ];

            // Step 1: Process changed investors (new sales/purchases)
            $metrics['investors_updated'] += $this->processChangedInvestors($state['last_run_time'], $this->startTime);

            // Step 2: Update investor sales data
            $this->updateInvestorSummaryData($state['last_run_time']);

            // Step 3: Update market activity for active areas
            $this->updateMarketActivity($state['last_run_time']);

            // Step 4: Update search index for changed entities
            $this->updateSearchIndex($state['last_run_time']);


            // Step 5: Recalculate scores for affected investors
            $this->recalculateScores($state['last_run_time']);

            // Step 6: Clean up old/inactive data
            // $this->cleanupInactiveData();

            // Save state for next run
            $newState = [
                'last_run_time' => date('Y-m-d'),
                'last_property_id' => $this->getMaxPropertyId(),
                'total_processed' => array_sum($metrics),
                'metrics' => $metrics
            ];
            $this->saveRunState($newState);

            $duration = round(microtime(true) - $this->startTime, 2);
            $this->log("=== ETL Complete in {$duration}s ===");
            $this->log("Metrics: " . json_encode($metrics));

        } catch (Exception $e) {
            $this->log("ETL Failed: " . $e->getMessage(), 'ERROR');
            // Don't update state on failure - will retry same data next run
            throw $e;
        }
    }

    /**
     * Process investors with recent activity (both purchases and sales)
     */
    private function processChangedInvestors($lastRunTime, $startTime)
    {
        $this->log("Processing changed investors since $lastRunTime...");
        $statesInQuery = "('PA', 'MD', 'VA', 'NJ', 'WV', 'DE', 'DC')";
        $onlyNewInvestors = false;
        // $statesInQuery = "('PA')";

        $sql = " SELECT * FROM (
        -- First, get investors who made recent purchases
        SELECT
            CONCAT(
              COALESCE(dp.MailingFullStreetAddress, ''), ' ',
              COALESCE(dp.MailingCity, ''), ' ',
              COALESCE(dp.MailingState, ''), ' ',
              COALESCE(dp.MailingZIP5, '')
            ) AS mailing_key,
            MAX(dp.MailingFullStreetAddress) AS mailing_address,
            MAX(dp.MailingCity) AS mailing_city,
            MAX(dp.MailingState) AS mailing_state,
            MAX(dp.MailingZIP5) AS mailing_zip5,
            GROUP_CONCAT(DISTINCT dp.OwnerNAME1FULL SEPARATOR '||') AS entity_names_str,
            MAX(CASE
                    WHEN dp.Owner1CorpInd IN ('Y','T','1') THEN 1
                    WHEN UPPER(dp.OwnerNAME1FULL) REGEXP 'LLC|INC|CORP|TRUST' THEN 1
                    ELSE 0
                END) AS is_corporate,
            COUNT(DISTINCT dp.PropertyID) AS properties_total,
            SUM(CASE WHEN dp.OwnerOccupied = 'N' THEN 1 ELSE 0 END) AS properties_absentee,
            SUM(CASE
                    WHEN dp.ConcurrentMtg1LoanAmt IS NULL OR dp.ConcurrentMtg1LoanAmt = 0
                        THEN 1 ELSE 0
                END) AS cash_purchases,
            SUM(dp.CurrentSalesPrice) AS total_investment,
            AVG(dp.CurrentSalesPrice) AS avg_property_value,
            -- MIN(dp.CurrentSaleRecordingDate) AS first_purchase,
            MAX(dp.CurrentSaleRecordingDate) AS last_purchase,
            'PURCHASE' AS activity_type,
            MIN(dp.PropertyID) AS first_property_id
        FROM datatree_property dp
        WHERE
            (dp.Owner1CorpInd IN ('Y','T','1')
                OR UPPER(dp.OwnerNAME1FULL) REGEXP 'LLC|INC|CORP|TRUST|PROPERTIES|CAPITAL')
         -- AND dp.MailingFullStreetAddress = '1309 MACDADE BLVD' -- Test one TEST ADDRESS:::::
          AND dp.MailingFullStreetAddress IS NOT NULL
          AND dp.MailingFullStreetAddress != ''
          AND dp.MailingCity IS NOT NULL
          AND dp.MailingCity != ''
          AND dp.MailingState IS NOT NULL
          AND dp.MailingState != ''
          AND dp.MailingZIP5 IS NOT NULL
          AND dp.MailingZIP5 != ''
          AND dp.SitusState IN $statesInQuery
          AND dp.CurrentSaleRecordingDate IS NOT NULL
          AND dp.CurrentSaleRecordingDate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          AND dp.CurrentSaleRecordingDate >= :last_run_time
        GROUP BY
            dp.MailingFullStreetAddress,
            dp.MailingCity,
            dp.MailingState,
            dp.MailingZIP5
        HAVING properties_total >= 1
        
        UNION ALL
        
        -- Get investors who made recent sales (using seller's historical mailing address)
        SELECT
            -- ✅ Use the SELLER's mailing address from when they owned the property
            CONCAT(
              COALESCE(seller_info.MailingFullStreetAddress, ''), ' ',
              COALESCE(seller_info.MailingCity, ''), ' ',
              COALESCE(seller_info.MailingState, ''), ' ',
              COALESCE(seller_info.MailingZIP5, '')
            ) AS mailing_key,
            MAX(seller_info.MailingFullStreetAddress) AS mailing_address,
            MAX(seller_info.MailingCity) AS mailing_city,
            MAX(seller_info.MailingState) AS mailing_state,
            MAX(seller_info.MailingZIP5) AS mailing_zip5,
            GROUP_CONCAT(DISTINCT dp.CurrentSaleSeller1FullName SEPARATOR '||') AS entity_names_str,
            MAX(CASE
                    WHEN UPPER(dp.CurrentSaleSeller1FullName) REGEXP 'LLC|INC|CORP|TRUST' THEN 1
                    ELSE 0
                END) AS is_corporate,
            COUNT(DISTINCT dp.PropertyID) AS properties_total,
            0 AS properties_absentee,
            0 AS cash_purchases,
            SUM(dp.CurrentSalesPrice) AS total_investment,
            AVG(dp.CurrentSalesPrice) AS avg_property_value,
            -- MIN(dp.CurrentSaleRecordingDate) AS first_purchase,
            MAX(dp.CurrentSaleRecordingDate) AS last_purchase,
            'SALE' AS activity_type,
            MIN(dp.PropertyID) AS first_property_id
        FROM datatree_property dp
        -- ✅ JOIN to get seller's mailing address from when they owned properties
        INNER JOIN (
            SELECT DISTINCT
                dt2.OwnerNAME1FULL,
                dt2.MailingFullStreetAddress,
                dt2.MailingCity,
                dt2.MailingState,
                dt2.MailingZIP5
            FROM datatree_property dt2
            WHERE dt2.SitusState IN $statesInQuery
              -- AND dt2.MailingFullStreetAddress = '1309 MACDADE BLVD' -- Test one TEST ADDRESS:::::
              AND dt2.MailingFullStreetAddress IS NOT NULL
              AND dt2.MailingFullStreetAddress != ''
              AND dt2.MailingCity IS NOT NULL
              AND dt2.MailingCity != ''
              AND dt2.MailingState IS NOT NULL
              AND dt2.MailingState != ''
              AND dt2.MailingZIP5 IS NOT NULL
              AND dt2.MailingZIP5 != ''
              AND (dt2.Owner1CorpInd IN ('Y','T','1')
                  OR UPPER(dt2.OwnerNAME1FULL) REGEXP 'LLC|INC|CORP|TRUST|PROPERTIES|CAPITAL')
              AND dt2.OwnerNAME1FULL IS NOT NULL
        ) AS seller_info ON dp.CurrentSaleSeller1FullName = seller_info.OwnerNAME1FULL
        WHERE
            dp.CurrentSaleSeller1FullName IS NOT NULL
          AND dp.CurrentSaleSeller1FullName != ''
          AND dp.SitusState IN $statesInQuery
          AND dp.CurrentSaleRecordingDate IS NOT NULL
          AND dp.CurrentSaleRecordingDate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          AND dp.CurrentSaleRecordingDate >= :last_run_time
        GROUP BY
            seller_info.MailingFullStreetAddress,  -- ✅ Group by SELLER's address
            seller_info.MailingCity,
            seller_info.MailingState,
            seller_info.MailingZIP5
        HAVING properties_total >= 1 -- at least 2 properties owned by the seller
    ) AS combined_results
    ORDER BY activity_type, 
             mailing_key
    LIMIT :batch_size OFFSET :offset
";

        // UPSERT into PostgreSQL (update if exists, insert if new)
        $upsertStmt = $this->pgConn->prepare("
    INSERT INTO investor_cache.investors (
        mailing_key, mailing_address, mailing_city, mailing_state,
        mailing_zip5, entity_names, primary_name, is_corporate,
        updated_at, last_activity_date
    ) VALUES (
        :mailing_key, :mailing_address, :mailing_city, :mailing_state,
        :mailing_zip5, :entity_names, :primary_name, :is_corporate,
        NOW(), :last_activity_date
    )
    ON CONFLICT (mailing_key) DO UPDATE SET
        entity_names = CASE 
            WHEN EXCLUDED.entity_names IS NOT NULL AND EXCLUDED.entity_names != '{}' THEN
                CASE 
                    WHEN investor_cache.investors.entity_names IS NULL OR investor_cache.investors.entity_names = '{}' THEN EXCLUDED.entity_names
                    ELSE array(SELECT DISTINCT unnest(investor_cache.investors.entity_names || EXCLUDED.entity_names) ORDER BY 1)
                END
            ELSE investor_cache.investors.entity_names
        END,
        mailing_address = EXCLUDED.mailing_address,
        mailing_city = EXCLUDED.mailing_city,
        mailing_state = EXCLUDED.mailing_state,
        mailing_zip5 = EXCLUDED.mailing_zip5,
        primary_name = EXCLUDED.primary_name,
        is_corporate = EXCLUDED.is_corporate,
        updated_at = NOW(),
        last_activity_date = EXCLUDED.last_activity_date
    RETURNING investor_id, 
              (xmax = 0) as inserted
");

        $offset = 0;
        $totalProcessed = 0;
        $totalInserted = 0;
        $totalUpdated = 0;
        $batchIndex = 0;
        do { //  Each batch is processed one by one
            $batchIndex++;
            $stmt = $this->mysqlConn->prepare($sql);
            $stmt->bindValue(':last_run_time', $lastRunTime);
            $stmt->bindValue(':batch_size', $this->batchSize, PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $rowCount = count($rows);

            if ($this->logging) {
                $this->printLogging("Processing: Batch " . $batchIndex . " (" . $totalProcessed . " - " . ($totalProcessed + $rowCount) . ") investors total time taken: " . round(microtime(true) - $startTime, 2) . "s");
            }

            $rowIndex = 0;
            foreach ($rows as $row) {

                $rowIndex++;
                // ignore if the mailing_key is already in the database - for test!
                // Check if this mailing_key already has records of this activity_type
                $activityType = $row['activity_type']; // SALE or PURCHASE
                $mailingKey = $row['mailing_key'];

                if ($onlyNewInvestors) {
                    $checkStmt = $this->pgConn->prepare("
                    SELECT COUNT(*) as count 
                    FROM investor_cache.properties p 
                    JOIN investor_cache.investors i ON p.investor_id = i.investor_id 
                    WHERE i.mailing_key = ? AND p.activity_type = ?
                ");
                    $checkStmt->execute([$mailingKey, $activityType]);
                    $result = $checkStmt->fetch(PDO::FETCH_ASSOC);

                    if ($result['count'] > 0) {
                        print (PHP_EOL . "Skipping: " . ($rowIndex + $totalProcessed) . " - " . $mailingKey . " - exists " . $activityType . " records");
                        continue;
                    }
                }

                if ($this->logging) {
                    print (PHP_EOL . "Processing: " . ($rowIndex + $totalProcessed) . " of " . ($totalProcessed + $rowCount) . " - " . $row['mailing_key']);
                }

                try {
                    $entityNames = explode('||', $row['entity_names_str']);
                    $pgArray = '{' . implode(',', array_map(function ($name) {
                        return '"' . str_replace('"', '\"', $name) . '"';
                    }, $entityNames)) . '}';

                    $isCorpValue = $this->convertToBoolean($row['is_corporate'] ?? '');

                    $upsertStmt->execute([
                        'mailing_key' => $row['mailing_key'],
                        'mailing_address' => $row['mailing_address'],
                        'mailing_city' => $row['mailing_city'],
                        'mailing_state' => $row['mailing_state'],
                        'mailing_zip5' => $row['mailing_zip5'],
                        'entity_names' => $pgArray,
                        'primary_name' => $entityNames[0],
                        'is_corporate' => (int) $isCorpValue,
                        'last_activity_date' => $row['last_purchase'] ?? null,
                    ]);

                    $result = $upsertStmt->fetch(PDO::FETCH_ASSOC);
                    if ($result['inserted']) {
                        $totalInserted++;
                    } else {
                        $totalUpdated++;
                    }

                    // Update properties for this investor (incremental)
                    $this->updateInvestorProperties($result['investor_id'], $row, $lastRunTime, $row['activity_type'], $statesInQuery);
                } catch (Exception $e) {
                    $this->log("Error updating investor properties: " . $e->getMessage(), 'ERROR');
                }
            }

            $offset += $this->batchSize;
            $totalProcessed += $rowCount;

            if ($totalProcessed % 1000 == 0) {
                $this->log("  Processed $totalProcessed investors (New: $totalInserted, Updated: $totalUpdated)");
            }

        } while ($rowCount == $this->batchSize);

        $this->log("  Total: $totalProcessed investors (New: $totalInserted, Updated: $totalUpdated)");
        return $totalProcessed;
    }

    /**
     * Update properties for a specific investor (incremental) - handles both purchases and sales
     */
    private function updateInvestorProperties($investorId, $investorData, $lastRunTime, $activityType, $statesInQuery)
    {
        if ($activityType === 'PURCHASE') {
            $this->processPurchaseProperties($investorId, $investorData, $lastRunTime);
        } elseif ($activityType === 'SALE') {
            $this->processSaleProperties($investorId, $investorData, $lastRunTime, $statesInQuery);
        }
    }

    /**
     * Process properties for purchases
     */
    private function processPurchaseProperties($investorId, $investorData, $lastRunTime)
    {
        // UPSERT properties purchased since lastRunTime (incremental processing)
        $sql = "
            SELECT 
                PropertyID, APN, FIPS,
                SitusFullStreetAddress, SitusCity,
                SitusState, SitusZIP5,
                SitusLatitude, SitusLongitude,
                BuildingClassCode, Bedrooms, BathFull,
                SumLivingAreaSqFt, YearBuilt,
                CurrentSaleRecordingDate, CurrentSalesPrice,
                (ConcurrentMtg1LoanAmt IS NULL OR ConcurrentMtg1LoanAmt = 0) as is_cash,
                (OwnerOccupied = 'N') as is_absentee,
                OwnerNAME1FULL,
                StyleCode,
                ConcurrentMtg1LoanAmt,
                ConcurrentMtg2LoanAmt,
                LotSizeSqFt
            FROM datatree_property
            WHERE MailingFullStreetAddress = ?
              AND MailingCity = ?
              AND MailingState = ?
              AND MailingZIP5 = ?
              AND DATE(CurrentSaleRecordingDate) >= ?
              AND CurrentSaleRecordingDate IS NOT NULL
              AND CurrentSaleRecordingDate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        ";

        $stmt = $this->mysqlConn->prepare($sql);
        $stmt->execute([
            $investorData['mailing_address'],
            $investorData['mailing_city'],
            $investorData['mailing_state'],
            $investorData['mailing_zip5'],
            $lastRunTime
        ]);

        $propUpsert = $this->pgConn->prepare("
            INSERT INTO investor_cache.properties (
                investor_id, datatree_id, apn, fips,
                situs_address, situs_city, situs_state, situs_zip5,
                location, property_class, bedrooms, bathrooms,
                sqft, year_built, purchase_date, purchase_price,
                cash_purchase, currently_owned, is_absentee, activity_type, entity_name,
                style_code, concurrent_mtg1_loan_amt, concurrent_mtg2_loan_amt, lot_size, property_type
            ) VALUES (
                :investor_id, :datatree_id, :apn, :fips,
                :situs_address, :situs_city, :situs_state, :situs_zip5,
                ST_MakePoint(:longitude, :latitude), :property_class, :bedrooms, :bathrooms,
                :sqft, :year_built, :purchase_date, :purchase_price,
                :cash_purchase, :currently_owned, :is_absentee, :activity_type, :entity_name,
                :style_code, :concurrent_mtg1_loan_amt, :concurrent_mtg2_loan_amt, :lot_size, :property_type
            )
            ON CONFLICT (datatree_id, investor_id) DO UPDATE SET
                investor_id = EXCLUDED.investor_id,
                purchase_price = EXCLUDED.purchase_price,
                purchase_date = EXCLUDED.purchase_date,
                currently_owned = TRUE,
                sale_date = NULL,
                activity_type = EXCLUDED.activity_type,
                entity_name = EXCLUDED.entity_name,
                style_code = EXCLUDED.style_code,
                concurrent_mtg1_loan_amt = EXCLUDED.concurrent_mtg1_loan_amt,
                concurrent_mtg2_loan_amt = EXCLUDED.concurrent_mtg2_loan_amt,
                lot_size = EXCLUDED.lot_size,
                property_type = EXCLUDED.property_type
        ");
        $inserted = 0;
        while ($prop = $stmt->fetch(PDO::FETCH_ASSOC)) {
            // Cast numeric values to proper types
            $bedrooms = $prop['Bedrooms'] !== null ? min(99, intval($prop['Bedrooms'])) : null;
            $bathrooms = $prop['BathFull'] !== null ? min(99, floatval($prop['BathFull'])) : null;
            $sqft = $prop['SumLivingAreaSqFt'] !== null ? intval($prop['SumLivingAreaSqFt']) : null;
            $yearBuilt = $prop['YearBuilt'] !== null ? intval($prop['YearBuilt']) : null;
            $purchasePrice = $prop['CurrentSalesPrice'] !== null ? floatval($prop['CurrentSalesPrice']) : null;
            $lotSize = $prop['LotSizeSqFt'] !== null ? intval($prop['LotSizeSqFt']) : null;
            $concurrentMtg1 = $prop['ConcurrentMtg1LoanAmt'] !== null ? floatval($prop['ConcurrentMtg1LoanAmt']) : null;
            $concurrentMtg2 = $prop['ConcurrentMtg2LoanAmt'] !== null ? floatval($prop['ConcurrentMtg2LoanAmt']) : null;

            // Cast boolean values properly - more robust conversion
            $isCash = $this->convertToBoolean($prop['is_cash'] ?? '');
            $isAbsentee = $this->convertToBoolean($prop['is_absentee'] ?? '');

            // Convert StyleCode to property_type
            $propertyType = null;
            if (!empty($prop['StyleCode'])) {
                $styleName = DatatreeAPIHelperExtended::convertStyleCode($prop['StyleCode']);
                $propertyType = DatatreeAPIHelperExtended::convertStyleToPropType($styleName);
            }

            $propUpsert->execute([
                'investor_id' => $investorId,
                'datatree_id' => $prop['PropertyID'],
                'apn' => $prop['APN'] ?? null,
                'fips' => $prop['FIPS'] ?? null,
                'situs_address' => $prop['SitusFullStreetAddress'] ?? null,
                'situs_city' => $prop['SitusCity'] ?? null,
                'situs_state' => $prop['SitusState'] ?? null,
                'situs_zip5' => $prop['SitusZIP5'] ?? null,
                'longitude' => $prop['SitusLongitude'] ?? null,
                'latitude' => $prop['SitusLatitude'] ?? null,
                'property_class' => $prop['BuildingClassCode'] ?? null,
                'bedrooms' => (int) $bedrooms,
                'bathrooms' => (float) $bathrooms,
                'sqft' => (int) $sqft,
                'year_built' => (int) $yearBuilt,
                'purchase_date' => $prop['CurrentSaleRecordingDate'] ?? null,
                'purchase_price' => (float) $purchasePrice ?? null,
                'cash_purchase' => (int) ($isCash ?? 0),
                'currently_owned' => 1,
                'is_absentee' => (int) ($isAbsentee ?? 0),
                'activity_type' => 'PURCHASE',
                'entity_name' => $prop['OwnerNAME1FULL'] ?? null,
                'style_code' => $prop['StyleCode'] ?? null,
                'concurrent_mtg1_loan_amt' => (float) $concurrentMtg1 ?? null,
                'concurrent_mtg2_loan_amt' => (float) $concurrentMtg2 ?? null,
                'lot_size' => (int) $lotSize ?? null,
                'property_type' => $propertyType
            ]);
            $inserted++;
        }
        print (" InvesterID: $investorId, Activity Type: PURCHASE, Properties: " . $inserted);
    }

    /**
     * Process properties for sales
     */
    private function processSaleProperties($investorId, $investorData, $lastRunTime, $statesInQuery)
    {
        // Get properties that this investor sold recently
        // Split entity names and create placeholders for MySQL IN clause

        $entityNames = explode('||', $investorData['entity_names_str']);
        $placeholders = str_repeat('?,', count($entityNames) - 1) . '?';

        $sql = "
            SELECT 
                PropertyID, APN, FIPS,
                SitusFullStreetAddress, SitusCity,
                SitusState, SitusZIP5,
                SitusLatitude, SitusLongitude, 
                BuildingClassCode, Bedrooms, BathFull,
                SumLivingAreaSqFt, YearBuilt,
                CurrentSaleRecordingDate, CurrentSalesPrice,
                (ConcurrentMtg1LoanAmt IS NULL OR ConcurrentMtg1LoanAmt = 0) as is_cash,
                (OwnerOccupied = 'N') as is_absentee,
                CurrentSaleSeller1FullName,
                StyleCode,
                ConcurrentMtg1LoanAmt,
                ConcurrentMtg2LoanAmt,
                LotSizeSqFt
            FROM datatree_property
            WHERE CurrentSaleSeller1FullName IN ($placeholders)
              AND CurrentSaleRecordingDate >= ?
              -- AND CurrentSaleRecordingDate IS NOT NULL
              -- AND CurrentSaleRecordingDate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            ORDER BY CurrentSaleRecordingDate DESC
            LIMIT 1000
        ";

        $stmt = $this->mysqlConn->prepare($sql);
        $params = array_merge($entityNames, [$lastRunTime]);
        $stmt->execute($params);

        $propUpsert = $this->pgConn->prepare("
            INSERT INTO investor_cache.properties (
                investor_id, datatree_id, apn, fips,
                situs_address, situs_city, situs_state, situs_zip5,
                location, property_class, bedrooms, bathrooms,
                sqft, year_built, purchase_date, purchase_price,
                sale_date, sale_price, cash_purchase, currently_owned, is_absentee, activity_type, entity_name,
                style_code, concurrent_mtg1_loan_amt, concurrent_mtg2_loan_amt, lot_size, property_type
            ) VALUES (
                :investor_id, :datatree_id, :apn, :fips,
                :situs_address, :situs_city, :situs_state, :situs_zip5,
                ST_MakePoint(:longitude, :latitude), :property_class, :bedrooms, :bathrooms,
                :sqft, :year_built, :purchase_date, :purchase_price,
                :sale_date, :sale_price, :cash_purchase, :currently_owned, :is_absentee, :activity_type, :entity_name,
                :style_code, :concurrent_mtg1_loan_amt, :concurrent_mtg2_loan_amt, :lot_size, :property_type
            )
            ON CONFLICT (datatree_id, investor_id) DO UPDATE SET
                sale_date = EXCLUDED.sale_date,
                sale_price = EXCLUDED.sale_price,
                currently_owned = FALSE,
                entity_name = EXCLUDED.entity_name,
                style_code = EXCLUDED.style_code,
                concurrent_mtg1_loan_amt = EXCLUDED.concurrent_mtg1_loan_amt,
                concurrent_mtg2_loan_amt = EXCLUDED.concurrent_mtg2_loan_amt,
                lot_size = EXCLUDED.lot_size,
                property_type = EXCLUDED.property_type
        ");
        $inserted = 0;
        while ($prop = $stmt->fetch(PDO::FETCH_ASSOC)) {
            // Cast numeric values to proper types
            $bedrooms = $prop['Bedrooms'] !== null ? min(99, intval($prop['Bedrooms'])) : null;
            $bathrooms = $prop['BathFull'] !== null ? min(99, floatval($prop['BathFull'])) : null;
            $sqft = $prop['SumLivingAreaSqFt'] !== null ? intval($prop['SumLivingAreaSqFt']) : null;
            $yearBuilt = $prop['YearBuilt'] !== null ? intval($prop['YearBuilt']) : null;
            $salePrice = $prop['CurrentSalesPrice'] !== null ? floatval($prop['CurrentSalesPrice']) : null;
            $lotSize = $prop['LotSizeSqFt'] !== null ? intval($prop['LotSizeSqFt']) : null;
            $concurrentMtg1 = $prop['ConcurrentMtg1LoanAmt'] !== null ? floatval($prop['ConcurrentMtg1LoanAmt']) : null;
            $concurrentMtg2 = $prop['ConcurrentMtg2LoanAmt'] !== null ? floatval($prop['ConcurrentMtg2LoanAmt']) : null;

            // Cast boolean values properly - more robust conversion
            $isCash = $this->convertToBoolean($prop['is_cash'] ?? '');
            $isAbsentee = $this->convertToBoolean($prop['is_absentee'] ?? '');

            // Convert StyleCode to property_type
            $propertyType = null;
            if (!empty($prop['StyleCode'])) {
                $styleName = DatatreeAPIHelperExtended::convertStyleCode($prop['StyleCode']);
                $propertyType = DatatreeAPIHelperExtended::convertStyleToPropType($styleName);
            }

            $propUpsert->execute([
                'investor_id' => $investorId,
                'datatree_id' => $prop['PropertyID'] ?? null,
                'apn' => $prop['APN'] ?? null,
                'fips' => $prop['FIPS'] ?? null,
                'situs_address' => $prop['SitusFullStreetAddress'] ?? null,
                'situs_city' => $prop['SitusCity'] ?? null,
                'situs_state' => $prop['SitusState'] ?? null,
                'situs_zip5' => $prop['SitusZIP5'] ?? null,
                'longitude' => $prop['SitusLongitude'] ?? null,
                'latitude' => $prop['SitusLatitude'] ?? null,
                'property_class' => $prop['BuildingClassCode'] ?? null,
                'bedrooms' => (int) $bedrooms,
                'bathrooms' => (float) $bathrooms,
                'sqft' => (int) $sqft,
                'year_built' => (int) $yearBuilt,
                'purchase_date' => null,
                'purchase_price' => null,
                'sale_date' => $prop['CurrentSaleRecordingDate'],
                'sale_price' => $salePrice,
                'cash_purchase' => (int) $isCash,             // ✅ int 0/1
                'currently_owned' => 0,                         // ✅ int 0
                'is_absentee' => (int) ($isAbsentee ?? 0),  // ✅ int 0/1
                'activity_type' => 'SALE',
                'entity_name' => $prop['CurrentSaleSeller1FullName'] ?? null,
                'style_code' => $prop['StyleCode'] ?? null,
                'concurrent_mtg1_loan_amt' => (float) $concurrentMtg1 ?? null,
                'concurrent_mtg2_loan_amt' => (float) $concurrentMtg2 ?? null,
                'lot_size' => (int) $lotSize ?? null,
                'property_type' => $propertyType
            ]);
            $inserted++;
        }
        print (" InvestorID: $investorId, Activity Type: SALE, Properties: " . $inserted);
    }

    /**
     * Update market activity for areas with recent changes (includes both purchases and sales)
     */
    private function updateMarketActivity($lastRunTime)
    {
        $this->log("Updating market activity...");

        // Update activity for areas with recent transactions (both purchases and sales)
        $this->pgConn->exec("
            INSERT INTO investor_cache.market_activity (
                investor_id, fips, zip5,
                properties_purchased, properties_sold, properties_owned,
                total_invested, total_sold_value,
                purchases_30d, purchases_90d, purchases_365d,
                sales_30d, sales_90d, sales_365d,
                last_purchase_date, last_sale_date,
                calculated_at
            )
            SELECT 
                p.investor_id,
                p.fips,
                p.situs_zip5,
                -- Purchases
                SUM(CASE WHEN p.purchase_date IS NOT NULL THEN 1 ELSE 0 END) as properties_purchased,
                -- Sales
                SUM(CASE WHEN p.sale_date IS NOT NULL THEN 1 ELSE 0 END) as properties_sold,
                -- Currently owned
                SUM(CASE WHEN p.currently_owned THEN 1 ELSE 0 END) as properties_owned,
                -- Investment values
                SUM(COALESCE(p.purchase_price, 0)) as total_invested,
                SUM(COALESCE(p.sale_price, 0)) as total_sold_value,
                -- Purchase timeframes
                SUM(CASE WHEN p.purchase_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END) as purchases_30d,
                SUM(CASE WHEN p.purchase_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 ELSE 0 END) as purchases_90d,
                SUM(CASE WHEN p.purchase_date >= CURRENT_DATE - INTERVAL '365 days' THEN 1 ELSE 0 END) as purchases_365d,
                -- Sales timeframes
                SUM(CASE WHEN p.sale_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END) as sales_30d,
                SUM(CASE WHEN p.sale_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 ELSE 0 END) as sales_90d,
                SUM(CASE WHEN p.sale_date >= CURRENT_DATE - INTERVAL '365 days' THEN 1 ELSE 0 END) as sales_365d,
                -- Last activity dates
                MAX(p.purchase_date) as last_purchase_date,
                MAX(p.sale_date) as last_sale_date,
                NOW() as calculated_at
            FROM investor_cache.properties p
            JOIN investor_cache.investors i ON p.investor_id = i.investor_id
            WHERE i.updated_at >= '$lastRunTime'::timestamp
            GROUP BY p.investor_id, p.fips, p.situs_zip5
            ON CONFLICT (investor_id, fips, zip5) DO UPDATE SET
                properties_purchased = EXCLUDED.properties_purchased,
                properties_sold = EXCLUDED.properties_sold,
                properties_owned = EXCLUDED.properties_owned,
                total_invested = EXCLUDED.total_invested,
                total_sold_value = EXCLUDED.total_sold_value,
                purchases_30d = EXCLUDED.purchases_30d,
                purchases_90d = EXCLUDED.purchases_90d,
                purchases_365d = EXCLUDED.purchases_365d,
                sales_30d = EXCLUDED.sales_30d,
                sales_90d = EXCLUDED.sales_90d,
                sales_365d = EXCLUDED.sales_365d,
                last_purchase_date = EXCLUDED.last_purchase_date,
                last_sale_date = EXCLUDED.last_sale_date,
                calculated_at = NOW()
        ");

        $this->log("  Market activity updated for recent transactions");
    }

    /**
     * Update investor sales data in the main investors table
     */
    private function updateInvestorSummaryData($lastRunTime)
    {
        $this->log("Updating investor sales data...");

        // Update all sales-related fields for investors who have recent sales
        $this->pgConn->exec("
              UPDATE investor_cache.investors i
            SET 
                properties_owned_total = (
                    SELECT COUNT(*)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.currently_owned = TRUE
                      AND p.activity_type = 'PURCHASE'
                ),

                properties_owned_absentee = (
                    SELECT COUNT(*)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.is_absentee = TRUE
                      AND p.activity_type = 'PURCHASE'
                ),
                total_investment = (
                    SELECT LEAST(COALESCE(SUM(p.purchase_price), 0), 999999999999.99)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.activity_type = 'PURCHASE'
                ),
                avg_property_value = (
                    SELECT LEAST(COALESCE(AVG(p.purchase_price), 0), 999999999999.99)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.activity_type = 'PURCHASE'
                ),
                last_purchase_date = (
                    SELECT MAX(p.purchase_date)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.activity_type = 'PURCHASE'
                ),
                properties_cash_purchased  = (
                    SELECT COUNT(*)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.activity_type = 'PURCHASE'
                      AND p.cash_purchase = TRUE
                ),
                first_purchase_date = (
                    SELECT MIN(p.purchase_date)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.activity_type = 'PURCHASE'
                ),
                days_inactive = (
                    SELECT 
                        (CURRENT_DATE - GREATEST(
                            COALESCE(MAX(p.purchase_date), '1900-01-01'::date),
                            COALESCE(MAX(p.sale_date), '1900-01-01'::date)
                        ))::int
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND (p.purchase_date IS NOT NULL OR p.sale_date IS NOT NULL)
                ),
                properties_sold_total = (
                    SELECT COUNT(*)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.sale_date IS NOT NULL
                      AND p.activity_type = 'SALE'
                ),
                total_sold_value = (
                    SELECT LEAST(COALESCE(SUM(p.sale_price), 0), 999999999999.99)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.sale_date IS NOT NULL
                      AND p.activity_type = 'SALE'
                ),
                last_sale_date = (
                    SELECT MAX(p.sale_date)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.sale_date IS NOT NULL
                      AND p.activity_type = 'SALE'
                ),
                last_activity_date = GREATEST(
                    COALESCE((
                        SELECT MAX(p.purchase_date)
                        FROM investor_cache.properties p
                        WHERE p.investor_id = i.investor_id
                          AND p.activity_type = 'PURCHASE'
                    ), '1900-01-01'::date),
                    COALESCE((
                        SELECT MAX(p.sale_date)
                        FROM investor_cache.properties p
                        WHERE p.investor_id = i.investor_id
                          AND p.sale_date IS NOT NULL
                    ), '1900-01-01'::date)
                )
            WHERE i.updated_at >= '$lastRunTime'::timestamp
        ");

        $affectedRows = $this->pgConn->query("
            SELECT COUNT(*) FROM investor_cache.investors 
            WHERE updated_at >= '$lastRunTime'::timestamp
        ")->fetchColumn();

        $this->log("  Updated sales data for $affectedRows investors");
    }

    private function getMaxPropertyId()
    {
        return $this->pgConn->query(
            "SELECT COALESCE(MAX(datatree_id), 0) FROM investor_cache.properties"
        )->fetchColumn();
    }

    /**
     * Convert MySQL boolean values to PostgreSQL boolean
     */
    private function convertToBoolean($value)
    {
        $value = trim((string) $value);

        if ($value === '' || $value === '0' || $value === 'false' || $value === 'FALSE' || $value === 'N' || $value === 'n') {
            return 0;
        }

        return 1;
    }


    /**
     * Step 4: Update search index for changed entities
     */
    private function updateSearchIndex($lastRunTime)
    {
        $this->log("Updating search index...");

        // Clear old search terms for updated investors
        $this->pgConn->exec("
        DELETE FROM investor_cache.name_search
        WHERE investor_id IN (
            SELECT investor_id 
            FROM investor_cache.investors 
            WHERE updated_at >= '$lastRunTime'::timestamp
        ) ");

        // Get updated investors
        $investors = $this->pgConn->query("
        SELECT investor_id, entity_names, primary_name 
        FROM investor_cache.investors
        WHERE updated_at >= '$lastRunTime'::timestamp
    ")->fetchAll(PDO::FETCH_ASSOC);

        $insertStmt = $this->pgConn->prepare("
        INSERT INTO investor_cache.name_search 
        (investor_id, search_term, original_name, match_type, relevance)
        VALUES (?, ?, ?, ?, ?)
    ");

        foreach ($investors as $investor) {
            // Parse entity names from PostgreSQL array
            $names = json_decode('{' . trim($investor['entity_names'], '{}') . '}');

            if (!is_array($names))
                continue;

            foreach ($names as $name) {
                // Insert exact match
                $insertStmt->execute([
                    $investor['investor_id'],
                    strtoupper(trim($name)),
                    $name,
                    'exact',
                    1.0
                ]);

                // Insert partial match (remove common suffixes)
                $partialName = preg_replace(
                    '/(LLC|INC|CORP|PROPERTIES|INVESTMENTS|CAPITAL|GROUP|TRUST|LP|PARTNERS|VENTURES|HOLDINGS).*$/i',
                    '',
                    $name
                );
                $partialName = trim($partialName);

                if ($partialName && $partialName != $name) {
                    $insertStmt->execute([
                        $investor['investor_id'],
                        strtoupper($partialName),
                        $name,
                        'partial',
                        0.8
                    ]);
                }
            }
        }

        $this->log("  Updated search index for " . count($investors) . " investors");
    }

    /**
     * Step 5: Recalculate scores for affected investors (includes sales data)
     */
    private function recalculateScores($lastRunTime)
    {
        $this->log("Recalculating investor scores...");

        // Update investor types and scores based on activity (including sales)
        $this->pgConn->exec("
        UPDATE investor_cache.investors i
        SET 
            investor_type = CASE
                -- Flipper: High turnover (lots of sales relative to purchases)
                WHEN i.properties_owned_total >= 3
                     AND i.days_inactive < 180
                     AND COALESCE(i.properties_sold_total, 0) >= 2
                     AND i.last_sale_date >= CURRENT_DATE - INTERVAL '365 days'
                     AND COALESCE(i.properties_sold_total, 0) >= (i.properties_owned_total * 0.3)
                THEN 'Flipper'
                
                -- Landlord: Large portfolio, low turnover (few sales)
                WHEN i.properties_owned_total >= 8
                     AND i.properties_owned_absentee >= 5
                     AND (i.last_sale_date IS NULL OR i.last_sale_date < CURRENT_DATE - INTERVAL '180 days')
                     AND COALESCE(i.properties_sold_total, 0) < (i.properties_owned_total * 0.2)
                THEN 'Landlord'
                
                -- Portfolio Builder: Actively acquiring (more purchases than sales)
                WHEN i.properties_owned_total >= 3
                     AND i.days_inactive <= 180
                     AND COALESCE(i.properties_sold_total, 0) < (i.properties_owned_total * 0.5)
                     AND i.last_purchase_date >= CURRENT_DATE - INTERVAL '365 days'
                THEN 'Portfolio Builder'
                
                -- Active Investor
                WHEN i.properties_owned_total >= 5
                THEN 'Active Investor'
                
                -- Small Investor
                ELSE 'Small Investor'
            END,
            
            activity_score = LEAST(100,
                (LEAST(i.properties_owned_total, 20) * 5) +
                (LEAST(i.properties_cash_purchased, 50) * 2) +
                (CASE WHEN i.is_corporate THEN 10 ELSE 0 END) +
                (LN(i.properties_owned_absentee + 1) * 5) +
                (30 * EXP(-i.days_inactive / 90.0)) +
                -- Bonus for sales activity (flipping)
                (LEAST(COALESCE(i.properties_sold_total, 0), 20) * 3) +
                -- Bonus for recent sales activity
                (CASE WHEN i.last_sale_date >= CURRENT_DATE - INTERVAL '90 days' THEN 5 ELSE 0 END) +
                -- Bonus for high-value sales
                (CASE WHEN COALESCE(i.total_sold_value, 0) > 1000000 THEN 3 ELSE 0 END)
            )
        WHERE i.updated_at >= '$lastRunTime'::timestamp
    ");

        $affectedRows = $this->pgConn->query("
        SELECT COUNT(*) FROM investor_cache.investors 
        WHERE updated_at >= '$lastRunTime'::timestamp
    ")->fetchColumn();

        $this->log("  Recalculated scores for $affectedRows investors");
    }

    /**
     * Step 6: Clean up old/inactive data
     */
    private function cleanupInactiveData()
    {
        // Temporarily disabled for performance
        return;

        $this->log("Cleaning up inactive data...");

        // Remove very old inactive investors (no activity in 3+ years)
        $cutoffDate = date('Y-m-d', strtotime('-3 years'));

        // First, count what will be removed
        $countToRemove = $this->pgConn->query("
        SELECT COUNT(*) 
        FROM investor_cache.investors 
        WHERE last_purchase_date < '$cutoffDate'
            AND last_activity_date < '$cutoffDate'
            AND properties_owned_total < 3
    ")->fetchColumn();

        if ($countToRemove > 0) {
            // Remove inactive investors and their related data (CASCADE will handle related tables)
            $this->pgConn->exec("
            DELETE FROM investor_cache.investors 
            WHERE last_purchase_date < '$cutoffDate'
                AND last_activity_date < '$cutoffDate'
                AND properties_owned_total < 3
        ");

            $this->log("  Removed $countToRemove inactive investors");
        } else {
            $this->log("  No inactive investors to remove");
        }

        // Clean up orphaned properties (safety check)
        $orphaned = $this->pgConn->exec("
        DELETE FROM investor_cache.properties 
        WHERE investor_id NOT IN (
            SELECT investor_id FROM investor_cache.investors
        )
    ");

        if ($orphaned > 0) {
            $this->log("  Cleaned $orphaned orphaned properties");
        }

        // Vacuum analyze for PostgreSQL optimization (optional, can be scheduled separately)
        // Note: This can be slow on large tables, consider running separately
        // $this->pgConn->exec("VACUUM ANALYZE investor_cache.investors");
        // $this->pgConn->exec("VACUUM ANALYZE investor_cache.properties");
    }

    private function log($message, $level = 'INFO', $executionTimeMs = null)
    {
        $timestamp = date('Y-m-d H:i:s');

        // Auto-calculate memory usage if not provided
        $memoryUsageMb = $this->getMemoryUsage()['current_mb'];

        // Calculate execution time in milliseconds
        $executionTimeMs = null;
        if ($this->startTime && is_numeric($this->startTime)) {
            $executionTimeMs = round((microtime(true) - $this->startTime) * 1000, 2);
        }


        $logEntry = "[$timestamp] [$level] $message [Memory: {$memoryUsageMb}MB]";
        if ($executionTimeMs !== null) {
            $logEntry .= " [Time: " . ($executionTimeMs > 1000 ? round($executionTimeMs / 1000, 2) . 's' : $executionTimeMs . 'ms') . "]";
        }
        $logEntry .= "\n";
        file_put_contents($this->logFile, $logEntry, FILE_APPEND);

        $this->addLogToDB($logEntry, $level, $executionTimeMs, $memoryUsageMb, null);
        echo $logEntry;
    }

    private function addLogToDB($message, $type = 'INFO', $executionTimeMs = null, $memoryUsageMb = null, $investorId = null)
    {
        $processId = $this->processId;
        $batchId = $this->batchId;
        try {
            // Validate required parameters
            if (empty($processId) || empty($batchId)) {
                throw new Exception("Missing processId or batchId");
            }

            $stmt = $this->pgConn->prepare("
                INSERT INTO investor_cache.logs (
                    message, type, process_id, batch_id, investor_id, 
                    execution_time_ms, memory_usage_mb, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
            ");

            $stmt->execute([
                $message,
                $type,
                $processId,
                $batchId,
                $investorId,
                $executionTimeMs ? (int) $executionTimeMs : null,
                $memoryUsageMb ? (int) round((float) $memoryUsageMb) : null
            ]);

        } catch (Exception $e) {
            // Fallback to file logging if database logging fails (avoid recursive calls)
            $timestamp = date('Y-m-d H:i:s');
            $errorLog = "[$timestamp] [ERROR] Failed to log to database: " . $e->getMessage() . "\n";
            $originalLog = "[$timestamp] [$type] $message\n";

            file_put_contents($this->logFile, $errorLog, FILE_APPEND);
            file_put_contents($this->logFile, $originalLog, FILE_APPEND);

            echo $errorLog;
            echo $originalLog;
        }
    }

    private function printLogging($message)
    {
        print (PHP_EOL . $message);
        $this->addLogToDB($message, 'Custom');
    }

    /**
     * Get detailed memory usage information
     */
    private function getMemoryUsage()
    {
        return [
            'current_mb' => round(memory_get_usage() / 1024 / 1024, 2),
            'peak_mb' => round(memory_get_peak_usage() / 1024 / 1024, 2),
            'current_real_mb' => round(memory_get_usage(true) / 1024 / 1024, 2),
            'peak_real_mb' => round(memory_get_peak_usage(true) / 1024 / 1024, 2),
            'limit_mb' => $this->getMemoryLimitMB()
        ];
    }

    /**
     * Get memory limit in MB
     */
    private function getMemoryLimitMB()
    {
        $limit = ini_get('memory_limit');
        if ($limit == -1) {
            return 'unlimited';
        }

        $unit = strtolower(substr($limit, -1));
        $value = (int) $limit;

        switch ($unit) {
            case 'g':
                return $value * 1024;
            case 'm':
                return $value;
            case 'k':
                return round($value / 1024, 2);
            default:
                return round($value / 1024 / 1024, 2);
        }
    }

    /**
     * Log memory usage with detailed information
     */
    private function logMemoryUsage($context = '')
    {
        $memory = $this->getMemoryUsage();
        $message = "Memory Usage $context - Current: {$memory['current_mb']}MB, Peak: {$memory['peak_mb']}MB, Limit: {$memory['limit_mb']}MB";
        $this->log($message, 'INFO');
    }
}

// Run incremental ETL
ini_set('memory_limit', '8G');  // Much less memory needed
set_time_limit(3600 * 24 * 3);  // 3 days max

$etl = new IncrementalInvestorETL();
$etl->run();

include_once 'calculate_arv_avm.php'; // this is the new file to calculate the arv and avm

/* ADD THIS IF I WANT BUT CURRENTLY NO NEEDED 

-- Trigger: update_investors_timestamp

-- DROP TRIGGER IF EXISTS update_investors_timestamp ON investor_cache.investors;

CREATE OR REPLACE TRIGGER update_investors_timestamp
    BEFORE UPDATE 
    ON investor_cache.investors
    FOR EACH ROW
    EXECUTE FUNCTION public.update_updated_at();

    */
