<?php
// Backfill script for missing property fields
// Purpose: Update existing PostgreSQL records with style_code, concurrent_mtg1_loan_amt, 
//          concurrent_mtg2_loan_amt, lot_size, and property_type from MySQL

include_once 'config.inverstercache.php';
include_once __DIR__ . '/../../includes/DatatreeAPIHelperExtended.php';

class PropertyFieldsBackfill
{
    private $mysqlConn;
    private $pgConn;
    private $batchSize = 50000;  // MySQL batch size
    private $logFile = '/var/www/html/cron/API/buyers/investor-caching/logs/backfill_property_fields.log';
    private $missingIdsFile = '/var/www/html/cron/API/buyers/investor-caching/logs/missing_property_ids.txt';
    private $startTime;
    private $processId;
    private $knownMissingIds = [];  // Cache of known missing IDs

    // Statistics
    private $totalProcessed = 0;
    private $totalUpdated = 0;
    private $totalSkipped = 0;
    private $totalMissing = 0;
    private $totalErrors = 0;

    public function __construct()
    {
        $this->startTime = microtime(true);
        $this->processId = 'BACKFILL_' . date('YmdHis') . '_' . rand(100, 999);
        $this->initConnections();
        $this->loadKnownMissingIds();
        $this->log("=== Property Fields Backfill Started ===");
        $this->log("Process ID: {$this->processId}");
        $this->log("Loaded " . count($this->knownMissingIds) . " known missing IDs from previous runs");
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
     * Load known missing IDs from file
     */
    private function loadKnownMissingIds()
    {
        if (!file_exists($this->missingIdsFile)) {
            $this->knownMissingIds = [];
            return;
        }

        $content = file_get_contents($this->missingIdsFile);
        if ($content === false) {
            $this->knownMissingIds = [];
            return;
        }

        $lines = explode("\n", trim($content));
        $this->knownMissingIds = array_filter(array_map('trim', $lines), function($id) {
            return !empty($id) && is_numeric($id);
        });

        // Convert to associative array for faster lookup
        $this->knownMissingIds = array_flip($this->knownMissingIds);
    }

    /**
     * Add missing IDs to file (only if not already present)
     */
    private function saveMissingIds($missingIds)
    {
        if (empty($missingIds)) {
            return;
        }

        $newMissingIds = [];
        foreach ($missingIds as $id) {
            // Only add if not already in known missing IDs
            if (!isset($this->knownMissingIds[$id])) {
                $newMissingIds[] = $id;
                $this->knownMissingIds[$id] = true; // Add to cache
            }
        }

        if (empty($newMissingIds)) {
            return; // All IDs already known
        }

        // Append new missing IDs to file
        $dir = dirname($this->missingIdsFile);
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }

        $content = implode("\n", $newMissingIds) . "\n";
        file_put_contents($this->missingIdsFile, $content, FILE_APPEND | LOCK_EX);
        
        $this->log("Added " . count($newMissingIds) . " new missing IDs to file");
    }

    /**
     * Get count of records that need backfilling
     * (records where any of the 4 new columns is NULL)
     */
    private function getTotalRecordsToProcess()
    {
        $sql = "
            SELECT COUNT(DISTINCT datatree_id)
            FROM investor_cache.properties
            WHERE style_code IS NULL
               OR concurrent_mtg1_loan_amt IS NULL
               OR concurrent_mtg2_loan_amt IS NULL
               OR lot_size IS NULL;

        ";
        return (int) $this->pgConn->query($sql)->fetchColumn();
    }

    /**
     * Get batch of datatree_id values that need backfilling
     */
    private function getBatchOfDatatreeIds($offset, $limit)
    {
        $sql = "
            SELECT DISTINCT datatree_id
            FROM investor_cache.properties
            WHERE style_code IS NULL
               OR concurrent_mtg1_loan_amt IS NULL
               OR lot_size IS NULL
            ORDER BY datatree_id
            LIMIT 50000
        ";

        $stmt = $this->pgConn->prepare($sql);
        // $stmt->bindValue(1, $limit, PDO::PARAM_INT);
        // $stmt->bindValue(2, $offset, PDO::PARAM_INT); 
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * Fetch property data from MySQL for given PropertyIDs
     */
    private function fetchFromMySQL($propertyIds)
    {
        if (empty($propertyIds)) {
            return [];
        }

        $placeholders = str_repeat('?,', count($propertyIds) - 1) . '?';
        
        $sql = "
            SELECT 
                PropertyID,
                StyleCode,
                ConcurrentMtg1LoanAmt,
                ConcurrentMtg2LoanAmt,
                LotSizeSqFt
            FROM datatree_property
            WHERE PropertyID IN ($placeholders)
        ";

        $stmt = $this->mysqlConn->prepare($sql);
        $stmt->execute($propertyIds);

        $results = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $results[$row['PropertyID']] = $row;
        }

        return $results;
    }

    /**
     * Update a single PostgreSQL record with new field values
     */
    private function updatePostgreSQLRecord($datatreeId, $mysqlData)
    {
        // Calculate property_type from StyleCode
        $propertyType = null;
        if (!empty($mysqlData['StyleCode'])) {
            try {
                $styleName = DatatreeAPIHelperExtended::convertStyleCode($mysqlData['StyleCode']);
                $propertyType = DatatreeAPIHelperExtended::convertStyleToPropType($styleName);
            } catch (Exception $e) {
                $this->log("Error converting StyleCode {$mysqlData['StyleCode']} to property_type: " . $e->getMessage(), 'WARNING');
            }
        }

        // Cast numeric values
        $styleCode = $mysqlData['StyleCode'] ?? null;
        $concurrentMtg1 = $mysqlData['ConcurrentMtg1LoanAmt'] !== null ? floatval($mysqlData['ConcurrentMtg1LoanAmt']) : null;
        $concurrentMtg2 = $mysqlData['ConcurrentMtg2LoanAmt'] !== null ? floatval($mysqlData['ConcurrentMtg2LoanAmt']) : null;
        $lotSize = $mysqlData['LotSizeSqFt'] !== null ? intval($mysqlData['LotSizeSqFt']) : null;

        $sql = "
            UPDATE investor_cache.properties
            SET 
                style_code = :style_code,
                concurrent_mtg1_loan_amt = :concurrent_mtg1_loan_amt,
                concurrent_mtg2_loan_amt = :concurrent_mtg2_loan_amt,
                lot_size = :lot_size,
                property_type = :property_type
            WHERE datatree_id = :datatree_id
        ";

        try {
            $stmt = $this->pgConn->prepare($sql);
            $stmt->execute([
                'datatree_id' => $datatreeId,
                'style_code' => $styleCode,
                'concurrent_mtg1_loan_amt' => $concurrentMtg1,
                'concurrent_mtg2_loan_amt' => $concurrentMtg2,
                'lot_size' => $lotSize,
                'property_type' => $propertyType
            ]);

            return $stmt->rowCount() > 0;
        } catch (Exception $e) {
            $this->log("Error updating datatree_id {$datatreeId}: " . $e->getMessage(), 'ERROR');
            return false;
        }
    }

    /**
     * Process a single batch
     */
    private function processBatch($datatreeIds, $batchNumber)
    {
        $this->log("Processing batch #{$batchNumber} with " . count($datatreeIds) . " records...");

        // Filter out known missing IDs before querying MySQL
        $idsToQuery = [];
        $knownMissingInBatch = [];
        
        foreach ($datatreeIds as $id) {
            if (isset($this->knownMissingIds[$id])) {
                $knownMissingInBatch[] = $id;
            } else {
                $idsToQuery[] = $id;
            }
        }

        // Log if we skipped known missing IDs
        if (!empty($knownMissingInBatch)) {
            $this->totalSkipped += count($knownMissingInBatch);
            $this->totalMissing += count($knownMissingInBatch);
            $this->log("Skipped " . count($knownMissingInBatch) . " known missing IDs (not querying MySQL)");
        }

        // Fetch data from MySQL only for IDs not in known missing list
        $mysqlData = [];
        if (!empty($idsToQuery)) {
            $mysqlData = $this->fetchFromMySQL($idsToQuery);
        }

        // Track which IDs were found in MySQL
        $foundIds = array_keys($mysqlData);
        $newMissingIds = array_diff($idsToQuery, $foundIds);

        // Save newly discovered missing IDs to file
        if (!empty($newMissingIds)) {
            $this->saveMissingIds($newMissingIds);
            $this->totalMissing += count($newMissingIds);
            $this->log("Missing in MySQL (" . count($newMissingIds) . " new): " . implode(', ', array_slice($newMissingIds, 0, 10)) . (count($newMissingIds) > 10 ? '...' : ''), 'WARNING');
        }

        // Update each record individually
        $batchUpdated = 0;
        $batchSkipped = 0;
        $batchErrors = 0;

        foreach ($datatreeIds as $datatreeId) {
            $this->totalProcessed++;

            // Skip if known missing or not found in MySQL
            if (isset($this->knownMissingIds[$datatreeId])) {
                // Already known as missing, skip
                $batchSkipped++;
                continue;
            }

            if (!isset($mysqlData[$datatreeId])) {
                // Not found in MySQL (should have been added to missing list above)
                $batchSkipped++;
                $this->totalSkipped++;
                continue;
            }

            // Update PostgreSQL record
            $updated = $this->updatePostgreSQLRecord($datatreeId, $mysqlData[$datatreeId]);

            if ($updated) {
                $batchUpdated++;
                $this->totalUpdated++;
            } else {
                $batchErrors++;
                $this->totalErrors++;
            }

            // Progress logging every 100 records
            if ($this->totalProcessed % 100 == 0) {
                $this->log("Progress: {$this->totalProcessed} processed, {$this->totalUpdated} updated, {$this->totalSkipped} skipped, {$this->totalMissing} missing, {$this->totalErrors} errors");
            }
        }

        $this->log("Batch #{$batchNumber} complete: {$batchUpdated} updated, {$batchSkipped} skipped, {$batchErrors} errors");
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
                $this->log("No records need backfilling. All records already have the required fields.");
                return;
            }

            $offset = 0;
            $batchNumber = 0;

            // Process in batches
            while (true) {
                $batchNumber++;
                $datatreeIds = $this->getBatchOfDatatreeIds($offset, $this->batchSize);

                if (empty($datatreeIds)) {
                    break; // No more records to process
                }

                $this->processBatch($datatreeIds, $batchNumber);

                $offset += $this->batchSize;
 
                // Memory cleanup
                unset($datatreeIds);

                // Progress summary every 10 batches
                if ($batchNumber % 10 == 0) {
                    $duration = round(microtime(true) - $this->startTime, 2);
                    $this->log("=== Progress Summary (after {$batchNumber} batches) ===");
                    $this->log("Total processed: " . number_format($this->totalProcessed));
                    $this->log("Total updated: " . number_format($this->totalUpdated));
                    $this->log("Total skipped: " . number_format($this->totalSkipped));
                    $this->log("Total missing in MySQL: " . number_format($this->totalMissing));
                    $this->log("Total errors: " . number_format($this->totalErrors));
                    $this->log("Time elapsed: {$duration}s");
                    $this->log("Estimated remaining: " . $this->estimateRemainingTime($this->totalProcessed, $totalRecords, $duration));
                }
            }

            // Final summary
            $duration = round(microtime(true) - $this->startTime, 2);
            $this->log("=== Backfill Complete ===");
            $this->log("Total processed: " . number_format($this->totalProcessed));
            $this->log("Total updated: " . number_format($this->totalUpdated));
            $this->log("Total skipped: " . number_format($this->totalSkipped));
            $this->log("Total missing in MySQL: " . number_format($this->totalMissing));
            $this->log("Total errors: " . number_format($this->totalErrors));
            $this->log("Total time: {$duration}s (" . round($duration / 60, 2) . " minutes)");

        } catch (Exception $e) {
            $this->log("Fatal error: " . $e->getMessage(), 'ERROR');
            $this->log("Stack trace: " . $e->getTraceAsString(), 'ERROR');
            throw $e;
        }
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

// Run backfill
ini_set('memory_limit', '2G');
set_time_limit(0);  // No time limit

$backfill = new PropertyFieldsBackfill();
$backfill->run();

