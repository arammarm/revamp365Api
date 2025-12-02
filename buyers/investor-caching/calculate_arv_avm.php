<?php
// Calculate ARV and AVM for properties in investor_cache.properties
// Uses external properties table (from config.php) to find comparable sales

include_once 'config.inverstercache.php';
require_once __DIR__ . '/../../../config.php'; // External DB connection

class ARVAVMCalculator
{
    private $externalDbConn;  // External DB (from config.php) - for finding comps
    private $currentDbConn;  // Current DB (investor_cache) - for properties to update
    private $logFile = '/var/www/html/cron/API/buyers/investor-caching/logs/calculate_arv_avm.log';
    private $startTime;
    private $processId;

    // Statistics
    private $totalProcessed = 0;
    private $totalUpdated = 0;
    private $totalSkipped = 0;
    private $totalNoComps = 0;
    private $totalErrors = 0;

    public function __construct()
    {
        $this->startTime = microtime(true);
        $this->processId = 'ARVAVM_' . date('YmdHis') . '_' . rand(100, 999);
        $this->initConnections();
        $this->log("=== ARV/AVM Calculation Started ===");
        $this->log("Process ID: {$this->processId}");
    }

    private function initConnections()
    {
        // External DB connection (from config.php) - for finding comps
        global $db;
        $this->externalDbConn = $db;

        // Current DB connection (investor_cache) - for properties to update
        $this->currentDbConn = new PDO(
            "pgsql:host=" . PG_HOST . ";port=" . PG_PORT . ";dbname=" . PG_DB,
            PG_USER,
            PG_PASS,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
    }

    /**
     * Get count of records that need ARV/AVM calculation
     */
    private function getTotalRecordsToProcess()
    {
        $sql = "
            SELECT COUNT(*) 
            FROM investor_cache.properties
            WHERE (calculated IS NULL OR calculated = false)
              -- AND (arv IS NULL OR avm IS NULL OR accuracy_score IS NULL)
              AND situs_zip5 IS NOT NULL
              AND location IS NOT NULL
              AND sqft IS NOT NULL
              AND sqft > 0
              AND (activity_type = 'PURCHASE' AND purchase_price IS NOT NULL)
        ";
        return (int) $this->currentDbConn->query($sql)->fetchColumn();
    }



    private function getSelectedDatatreeIds($limit = 1000)
    {
        $sql = "
        SELECT  datatree_id
        FROM investor_cache.properties
        WHERE datatree_id IN (
            116748161,
            142499015,
            142697519,
            113864547,
            86855898,
            62884988,
            113354058,
            85648488,
            86764414,
            84495087,
            26507135,
            85644677,
            62881980,
            85043135,
            114236205,
            114236821,
            114243731,
            115115684,
            114372349,
            114232731,
            115167636,
            115115645,
            115131385,
            114227759,
            84539225,
            114258938,
            114236818,
            86800209,
            86800175,
            114328063,
            114252255,
            114238115,
            114238116,
            114236234,
            140716469,
            114236245,
            140716439,
            86806015,
            141943367
        )
        LIMIT $limit
    ";

        $stmt = $this->currentDbConn->prepare($sql);
        // $stmt->bindValue(1, $limit, PDO::PARAM_INT); // this makes the query slow
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_COLUMN);
    }
    /**
     * Get unique datatree_id values that need ARV/AVM calculation
     */
    private function getDatatreeIdsToProcess($limit = 1000)
    {

        $sql = "
            SELECT DISTINCT datatree_id
            FROM investor_cache.properties
            WHERE calculated = false
              AND situs_zip5 IS NOT NULL
              AND location IS NOT NULL
              AND sqft IS NOT NULL
              AND sqft > 0
              AND (activity_type = 'PURCHASE' AND purchase_price IS NOT NULL)
            ORDER BY datatree_id
            LIMIT $limit
        ";

        $stmt = $this->currentDbConn->prepare($sql);
        // $stmt->bindValue(1, $limit, PDO::PARAM_INT); // this makes the query slow
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * Get a sample property record for a datatree_id (for calculation)
     */
    private function getPropertyByDatatreeId($datatreeId)
    {
        $sql = "
            SELECT 
                property_id,
                datatree_id,
                situs_zip5,
                property_type,
                location,
                sqft,
                purchase_price,
                sale_price,
                activity_type,
                ST_Y(location::geometry) as latitude,
                ST_X(location::geometry) as longitude
            FROM investor_cache.properties
            WHERE datatree_id = ?
              AND situs_zip5 IS NOT NULL
              AND location IS NOT NULL
              AND sqft IS NOT NULL
              AND sqft > 0
            LIMIT 1
        ";

        $stmt = $this->currentDbConn->prepare($sql);
        $stmt->execute([$datatreeId]);
        return $stmt->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * Check if datatree_id already has calculated values
     */
    private function getExistingCalculatedValues($datatreeId)
    {
        return false;
        $sql = "
            SELECT arv, avm, accuracy_score
            FROM investor_cache.properties
            WHERE datatree_id = ?
              AND calculated = true
              AND arv IS NOT NULL
              AND avm IS NOT NULL
            LIMIT 1
        ";

        $stmt = $this->currentDbConn->prepare($sql);
        $stmt->execute([$datatreeId]);
        return $stmt->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * Update all properties with the same datatree_id
     */
    private function updateAllByDatatreeId($datatreeId, $arv, $avm, $accuracyScore)
    {
        $sql = "
            UPDATE investor_cache.properties
            SET 
                arv = :arv,
                avm = :avm,
                accuracy_score = :accuracy_score,
                calculated = true
            WHERE datatree_id = :datatree_id
              AND (calculated IS NULL OR calculated = false)
        ";

        try {
            $stmt = $this->currentDbConn->prepare($sql);
            $stmt->execute([
                'datatree_id' => $datatreeId,
                'arv' => $arv,
                'avm' => $avm,
                'accuracy_score' => $accuracyScore
            ]);

            return $stmt->rowCount();
        } catch (Exception $e) {
            $this->log("Error updating datatree_id {$datatreeId}: " . $e->getMessage(), 'ERROR');
            return 0;
        }
    }

    /**
     * Get structure types array based on property_type
     */
    private function getStructureTypes($propertyType)
    {
        // Handle NULL or empty property_type - default to 'Detached'
        if (empty($propertyType) || trim($propertyType) === '') {
            $propertyType = 'Other';
        }

        // Map property_type to structure_type values used in external properties table
        $propertyMapping = [
            'Detached' => ['Detached'],
            'Unit/Flat/Apartment' => ['Unit/Flat/Apartment', 'Penthouse Unit/Flat/Apartment'],
            'Penthouse Unit/Flat/Apartment' => ['Unit/Flat/Apartment', 'Penthouse Unit/Flat/Apartment'],
            'End of Row/Townhouse' => ['End of Row/Townhouse', 'Interior Row/Townhouse'],
            'Interior Row/Townhouse' => ['End of Row/Townhouse', 'Interior Row/Townhouse'],
            'Twin/Semi-Detached' => ['Twin/Semi-Detached'],
            'Manufactured' => ['Manufactured'],
            'Mobile Pre 1976' => ['Mobile Pre 1976'],
            'Garage/Parking Space' => ['Garage/Parking Space', 'Other'],
            'Other' => ['Garage/Parking Space', 'Other']
        ];

        return $propertyMapping[$propertyType] ?? ['Other'];
    }

    /**
     * Find comparable properties from external database
     */
    private function findComparableProperties($zipCode, $structureTypes, $lat, $lng, $sqft)
    {
        // Step 1: Get initial comps from same zip code with same structure type
        $compsQuery = "
            WITH avg_ppsqft AS (
                SELECT zip_code,
                       avg(price_per_sqft_closed) AS avg_ppsqft
                FROM properties
                WHERE status = $1
                  AND close_date::date > CURRENT_DATE - INTERVAL '6 months'
                  AND total_finished_sqft IS NOT NULL AND total_finished_sqft <> 0
                  AND price_per_sqft_closed IS NOT NULL AND price_per_sqft_closed <> 0
                GROUP BY zip_code
            )
            SELECT p.id,
                   p.structure_type,
                   p.latitude,
                   p.longitude,
                   p.total_finished_sqft,
                   p.price_per_sqft_closed,
                   p.close_price
            FROM properties p
            JOIN avg_ppsqft a ON a.zip_code = p.zip_code
            WHERE p.status = $1
              AND p.zip_code = $2
              AND p.close_date::date > CURRENT_DATE - INTERVAL '6 months'
              AND p.total_finished_sqft IS NOT NULL AND p.total_finished_sqft <> 0
              AND p.price_per_sqft_closed IS NOT NULL AND price_per_sqft_closed <> 0
              AND p.price_per_sqft_closed <= (3 * a.avg_ppsqft)
        ";

        $compsResults = pg_query_params($this->externalDbConn, $compsQuery, ['Closed', $zipCode]);
        if (!$compsResults) {
            return [];
        }

        $allComps = pg_fetch_all($compsResults);
        if (empty($allComps)) {
            return [];
        }

        // Step 2: Filter by structure type
        $filteredComps = [];
        foreach ($allComps as $comp) {
            if (in_array($comp['structure_type'], $structureTypes)) {
                $filteredComps[] = $comp;
            }
        }

        // Step 3: Filter by distance (within 1 mile) and sqft (80-120%)
        $matchedCompsIds = [];
        if (!empty($filteredComps)) {
            foreach ($filteredComps as $comp) {
                if ($this->isWithinMile($lat, $lng, $comp['latitude'], $comp['longitude'])) {
                    if ($this->isBetween80To120Percentage($sqft, $comp['total_finished_sqft'])) {
                        $matchedCompsIds[] = $comp['id'];
                    }
                }
            }
        }

        // Step 4: Calculate accuracy score (following get_calc_values.php logic)
        $accuracyScore = 9.9;
        $totalAccuracyScore = 0;
        $maximumRecordCheckCount = 10;

        if (count($matchedCompsIds) >= 10) {
            // High accuracy - perfect matches
            $totalAccuracyScore = count($matchedCompsIds) * 9.9;
        } else {
            // Start with existing matches
            $previousMatchIdsCount = count($matchedCompsIds);
            $totalAccuracyScore = $previousMatchIdsCount * 9.9;

            // Add comps with sqft match (80-120%) but maybe not perfect distance/structure
            $i = 1;
            foreach ($allComps as $comp) {
                if (
                    $this->isBetween80To120Percentage($sqft, $comp['total_finished_sqft'])
                    && (($i + $previousMatchIdsCount) <= $maximumRecordCheckCount)
                ) {
                    if (!in_array($comp['id'], $matchedCompsIds)) {
                        $matchedCompsIds[] = $comp['id'];
                        $totalAccuracyScore += 7.9;
                        $i++;
                    }
                }
            }

            // Add comps within 1 mile but maybe different structure
            $previousMatchIdsCount = count($matchedCompsIds);
            if ($previousMatchIdsCount < $maximumRecordCheckCount && $lat != null && $lng != null) {
                $i = 1;
                foreach ($allComps as $comp) {
                    if (
                        $this->isWithinMile($lat, $lng, $comp['latitude'], $comp['longitude'])
                        && (($i + $previousMatchIdsCount) <= $maximumRecordCheckCount)
                    ) {
                        if (!in_array($comp['id'], $matchedCompsIds)) {
                            $matchedCompsIds[] = $comp['id'];
                            $totalAccuracyScore += 5.9;
                            $i++;
                        }
                    }
                }
            }

            // Add comps with different structure type
            $previousMatchIdsCount = count($matchedCompsIds);
            if ($previousMatchIdsCount < $maximumRecordCheckCount) {
                $i = 1;
                foreach ($allComps as $comp) {
                    if (
                        !in_array($comp['structure_type'], $structureTypes)
                        && (($i + $previousMatchIdsCount) <= $maximumRecordCheckCount)
                    ) {
                        if (!in_array($comp['id'], $matchedCompsIds)) {
                            $matchedCompsIds[] = $comp['id'];
                            $totalAccuracyScore += 2.9;
                            $i++;
                        }
                    }
                }
            }
        }

        // Get final comps data
        if (empty($matchedCompsIds)) {
            return ['comps' => [], 'accuracy_score' => null];
        }

        $pgArrayIds = '{' . implode(', ', $matchedCompsIds) . '}';
        $finalCompsQuery = "
            WITH ranks AS (
                SELECT p.*,
                       row_number() OVER (ORDER BY p.price_per_sqft_closed DESC) AS price_per_sqft_rank
                FROM properties p
                WHERE p.id = ANY($1)
            )
            SELECT 
                r.id,
                r.price_per_sqft_closed,
                r.close_price,
                r.total_finished_sqft
            FROM ranks r
            ORDER BY r.price_per_sqft_closed DESC
        ";

        $finalCompsResult = pg_query_params($this->externalDbConn, $finalCompsQuery, [$pgArrayIds]);
        $finalComps = pg_fetch_all($finalCompsResult);

        // Calculate final accuracy score
        $finalAccuracy = 0;
        if (count($matchedCompsIds) > 0) {
            $finalAccuracy = round($totalAccuracyScore / count($matchedCompsIds), 2);
        }

        return [
            'comps' => $finalComps ?: [],
            'accuracy_score' => $finalAccuracy
        ];
    }

    /**
     * Calculate ARV and AVM from comps
     */
    private function calculateARVAVM($comps, $sqft)
    {
        if (empty($comps)) {
            return ['arv' => null, 'avm' => null];
        }

        // Calculate average price per sqft
        $totalPpsqft = 0;
        $count = 0;
        foreach ($comps as $comp) {
            if (!empty($comp['price_per_sqft_closed'])) {
                $totalPpsqft += floatval($comp['price_per_sqft_closed']);
                $count++;
            }
        }

        if ($count == 0) {
            return ['arv' => null, 'avm' => null];
        }

        $avgPricePerSqft = $totalPpsqft / $count;

        // Calculate high comp price per sqft (top 3)
        $highComps = array_slice($comps, 0, 3);
        $highCompsPpsqft = 0;
        $highCompsCount = 0;
        foreach ($highComps as $comp) {
            if (!empty($comp['price_per_sqft_closed'])) {
                $highCompsPpsqft += floatval($comp['price_per_sqft_closed']);
                $highCompsCount++;
            }
        }

        $highCompsAvgPpsqft = $highCompsCount > 0 ? $highCompsPpsqft / $highCompsCount : $avgPricePerSqft;

        // Calculate ARV and AVM
        $arv = $sqft * $highCompsAvgPpsqft;
        $avm = $sqft * $avgPricePerSqft;

        // Cap values at maximum for numeric(14,2) = 99,999,999,999,999.99 (10^14 - 0.01)
        $maxValue = 99999999999999.99;
        $arvRounded = round($arv, 2);
        $avmRounded = round($avm, 2);

        $arv = min($arvRounded, $maxValue);
        $avm = min($avmRounded, $maxValue);

        return [
            'arv' => $arv,
            'avm' => $avm
        ];
    }

    /**
     * Process a single datatree_id (all properties with same datatree_id)
     */
    private function processDatatreeId($datatreeId)
    {
        // Check if this datatree_id already has calculated values
        $existing = $this->getExistingCalculatedValues($datatreeId);

        if ($existing) {
            // Copy existing calculated values to all uncalculated records with this datatree_id
            $updated = $this->updateAllByDatatreeId(
                $datatreeId,
                $existing['arv'],
                $existing['avm'],
                $existing['accuracy_score']
            );

            if ($updated > 0) {
                $this->totalUpdated += $updated;
                $this->log("Datatree ID {$datatreeId}: Copied existing values to {$updated} records");
            }
            return;
        }

        // Get a sample property for calculation
        $property = $this->getPropertyByDatatreeId($datatreeId);

        if (!$property) {
            $this->totalSkipped++;
            return;
        }

        $this->totalProcessed++;

        // Get structure types
        $structureTypes = $this->getStructureTypes($property['property_type']);

        // Find comparable properties
        $compsData = $this->findComparableProperties(
            $property['situs_zip5'],
            $structureTypes,
            $property['latitude'],
            $property['longitude'],
            $property['sqft']
        );

        if (empty($compsData['comps'])) {
            $this->totalNoComps++;
            $this->log("Datatree ID {$datatreeId}: No comps found - setting to NULL");
            $this->updateAllByDatatreeId($datatreeId, null, null, null);
            return;
        }

     
        // Calculate ARV and AVM
        $calculated = $this->calculateARVAVM($compsData['comps'], $property['sqft']);

        // Update all properties with this datatree_id
        $updated = $this->updateAllByDatatreeId(
            $datatreeId,
            $calculated['arv'],
            $calculated['avm'],
            $compsData['accuracy_score']
        );

        if ($updated > 0) {
            $this->totalUpdated += $updated;
            $this->log("Datatree ID {$datatreeId}: Calculated ARV={$calculated['arv']}, AVM={$calculated['avm']}, Accuracy={$compsData['accuracy_score']}, Updated {$updated} records");
        } else {
            $this->totalErrors++;
        }

        // Progress logging every 100 datatree_ids
        if ($this->totalProcessed % 100 == 0) {
            $this->log("Progress: {$this->totalProcessed} datatree_ids processed, {$this->totalUpdated} records updated, {$this->totalNoComps} no comps, {$this->totalErrors} errors");
        }
    }

    /**
     * Main execution method
     */
    public function run()
    {
        try {
            // Get total count
            $totalRecords = $this->getTotalRecordsToProcess();
            $this->log("Total records to process: " . number_format($totalRecords));

            if ($totalRecords == 0) {
                $this->log("No records need ARV/AVM calculation. All records already have values.");
                return;
            }

            // Process by datatree_id groups
            while (true) {
                $datatreeIds = $this->getDatatreeIdsToProcess(1000); // Get 100 unique datatree_ids at a time

                if (empty($datatreeIds)) {
                    break; // No more records to process
                }

                foreach ($datatreeIds as $datatreeId) {
                    $this->processDatatreeId($datatreeId);
                }

                // Progress summary every 1000 datatree_ids
                if ($this->totalProcessed % 1000 == 0) {
                    $duration = round(microtime(true) - $this->startTime, 2);
                    $this->log("=== Progress Summary (after {$this->totalProcessed} datatree_ids) ===");
                    $this->log("Total records updated: " . number_format($this->totalUpdated));
                    $this->log("Total no comps: " . number_format($this->totalNoComps));
                    $this->log("Total errors: " . number_format($this->totalErrors));
                    $this->log("Time elapsed: {$duration}s");
                }
            }

            // Final summary
            $duration = round(microtime(true) - $this->startTime, 2);
            $this->log("=== Calculation Complete ===");
            $this->log("Total datatree_ids processed: " . number_format($this->totalProcessed));
            $this->log("Total records updated: " . number_format($this->totalUpdated));
            $this->log("Total no comps: " . number_format($this->totalNoComps));
            $this->log("Total errors: " . number_format($this->totalErrors));
            $this->log("Total time: {$duration}s (" . round($duration / 60, 2) . " minutes)");

        } catch (Exception $e) {
            $this->log("Fatal error: " . $e->getMessage(), 'ERROR');
            $this->log("Stack trace: " . $e->getTraceAsString(), 'ERROR');
            throw $e;
        }
    }

    /**
     * Check if distance is within 1 mile
     */
    private function isWithinMile($lat1, $lng1, $lat2, $lng2)
    {
        if ($lat1 == null || $lng1 == null || $lat2 == null || $lng2 == null) {
            return false;
        }

        $distance = $this->getGeoDistance($lat1, $lng1, $lat2, $lng2);
        return $distance <= 1.0;
    }

    /**
     * Calculate distance between two points in miles
     */
    private function getGeoDistance($lat1, $lon1, $lat2, $lon2, $unit = 'miles')
    {
        $earth_radius = ($unit === 'km') ? 6371.0 : 3958.8;

        // Convert degrees to radians
        $lat1 = deg2rad($lat1);
        $lon1 = deg2rad($lon1);
        $lat2 = deg2rad($lat2);
        $lon2 = deg2rad($lon2);

        // Haversine formula
        $dlat = $lat2 - $lat1;
        $dlon = $lon2 - $lon1;
        $a = sin($dlat / 2) ** 2 + cos($lat1) * cos($lat2) * sin($dlon / 2) ** 2;
        $c = 2 * atan2(sqrt($a), sqrt(1 - $a));

        return round($earth_radius * $c, 4);
    }

    /**
     * Check if sqft is within 80-120% range
     */
    private function isBetween80To120Percentage($subjectSqft, $compSqft)
    {
        $minSqft = ($subjectSqft / 100) * 80;
        $maxSqft = ($subjectSqft / 100) * 120;

        return ($compSqft >= $minSqft && $compSqft <= $maxSqft);
    }

    /**
     * Estimate remaining time
     */
    private function estimateRemainingTime($processed, $total, $elapsed)
    {
        if ($processed == 0) {
            return 'N/A';
        }

        $rate = $processed / $elapsed; // records per second
        $remaining = $total - $processed;
        $remainingSeconds = $remaining / $rate;

        if ($remainingSeconds < 60) {
            return round($remainingSeconds) . ' seconds';
        } elseif ($remainingSeconds < 3600) {
            return round($remainingSeconds / 60, 1) . ' minutes';
        } else {
            return round($remainingSeconds / 3600, 1) . ' hours';
        }
    }

    /**
     * Log message to file and console
     */
    private function log($message, $level = 'INFO')
    {
        $timestamp = date('Y-m-d H:i:s');
        $memoryUsageMb = round(memory_get_usage() / 1024 / 1024, 2);
        $logEntry = "[$timestamp] [$level] $message [Memory: {$memoryUsageMb}MB]";

        if ($this->startTime) {
            $elapsed = round(microtime(true) - $this->startTime, 2);
            $logEntry .= " [Elapsed: {$elapsed}s]";
        }

        $logEntry .= "\n";

        // Write to file
        file_put_contents($this->logFile, $logEntry, FILE_APPEND);

        // Output to console
        echo $logEntry;
    }
}

// Run calculation
ini_set('memory_limit', '2G');
set_time_limit(0);  // No time limit

$calculator = new ARVAVMCalculator();
$calculator->run();

