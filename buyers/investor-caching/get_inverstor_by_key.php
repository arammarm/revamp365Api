<?php
// File: /api/find_investor.php
// Response time: <500ms (from 15-30 seconds)

require_once 'config.inverstercache.php';
include_once "../../includes/APIHelperV2.php";
APIHelper::Authentication();

class FindInvestorAPI
{
    private $pgConn;

    public function __construct()
    {
        $dsn = "pgsql:host=" . PG_HOST .
            ";port=" . PG_PORT . ";dbname=" . PG_DB;
        $this->pgConn = new PDO(
            $dsn,
            PG_USER,
            PG_PASS,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
    }

    /**
     * Parse PostgreSQL text array format to PHP array
     * Handles format like: {"item1","item2","item3"}
     */
    private function parsePostgresArray($pgArray)
    {
        if (empty($pgArray) || $pgArray === '{}') {
            return [];
        }

        // Remove the curly braces
        $cleaned = trim($pgArray, '{}');

        // Handle empty array
        if (empty($cleaned)) {
            return [];
        }

        // Split by comma, but be careful with commas inside quoted strings
        $items = [];
        $current = '';
        $inQuotes = false;
        $escaped = false;

        for ($i = 0; $i < strlen($cleaned); $i++) {
            $char = $cleaned[$i];

            if ($escaped) {
                $current .= $char;
                $escaped = false;
            } elseif ($char === '\\') {
                $escaped = true;
            } elseif ($char === '"') {
                $inQuotes = !$inQuotes;
            } elseif ($char === ',' && !$inQuotes) {
                $items[] = trim($current, '"');
                $current = '';
            } else {
                $current .= $char;
            }
        }

        // Add the last item
        if (!empty($current)) {
            $items[] = trim($current, '"');
        }

        return array_filter($items); // Remove any empty items
    }

    public function search()
    {
        // Get search parameters - any one of these can be used
        $entity_names_str = APIHelper::GetParam('name', false);
        $mailingKey = APIHelper::GetParam('investor_identifier', false);
        $investorId = APIHelper::GetParam('investor_id', false);
        $property_limit = (int) APIHelper::GetParam('property_limit', false) ?: 15;

        $limit = APIHelper::GetParam('limit', false) ?: 50;

        // Determine which search parameter to use
        $searchType = null;
        $searchValue = null;

        if ($investorId) {
            $searchType = 'investor_id';
            $searchValue = $investorId;
        } elseif ($mailingKey) {
            $searchType = 'mailing_key';
            $searchValue = strtoupper(trim($mailingKey));
        } elseif ($entity_names_str) {
            $searchType = 'entity_name';
            $searchValue = strtoupper(trim($entity_names_str));
        } else {
            APIHelper::SendResponse(null, 0, "Please provide either 'name', 'investor_identifier', or 'investor_id' parameter");
            return;
        }

        // Simplified query using only the 3 main tables
        $sql = "
            SELECT 
                i.investor_id,
                i.primary_name,
                i.mailing_key,
                i.entity_names as all_names,
                i.mailing_address,
                i.mailing_city,
                i.mailing_state,
                i.mailing_zip5,
                i.is_corporate,
                i.investor_type,
                i.properties_owned_total,
                i.properties_owned_absentee,
                i.properties_cash_purchased,
                i.properties_sold_total,
                i.total_investment,
                i.total_sold_value,
                i.avg_property_value,
                i.first_purchase_date,
                i.last_purchase_date,
                i.last_sale_date,
                i.activity_score,
                i.days_inactive,
                i.last_activity_date,
                 
                -- Currently owned properties
                (
                    SELECT json_agg(
                        json_build_object(
                            'property_id', p.property_id,
                            'address', p.situs_address,
                            'city', p.situs_city,
                            'state', p.situs_state,
                            'zip', p.situs_zip5,
                            'purchase_date', p.purchase_date,
                            'purchase_price', p.purchase_price,
                            'bedrooms', p.bedrooms,
                            'bathrooms', p.bathrooms,
                            'sqft', p.sqft,
                            'year_built', p.year_built,
                            'property_class', p.property_class,
                            'property_type', p.property_type,
                            'lot_size', p.lot_size,
                            'style_code', p.style_code,
                            'concurrent_mtg1_loan_amt', p.concurrent_mtg1_loan_amt,
                            'concurrent_mtg2_loan_amt', p.concurrent_mtg2_loan_amt,
                            'avm', p.avm,
                            'arv', p.arv,
                            'accuracy_score', p.accuracy_score,
                            'entity_name', p.entity_name,
                            'cash_purchase', p.cash_purchase,
                            'is_absentee', p.is_absentee,
                            'lat', ST_Y(p.location::geometry),
                            'lng', ST_X(p.location::geometry)
                        )
                    )
                    FROM (
                        SELECT property_id, situs_address, situs_city, situs_state, situs_zip5,
                               purchase_date, purchase_price, bedrooms, bathrooms, sqft,
                               year_built, property_class, cash_purchase, is_absentee, location, property_type, lot_size, style_code, concurrent_mtg1_loan_amt, concurrent_mtg2_loan_amt, avm, arv, accuracy_score, entity_name 
                        FROM investor_cache.properties p
                        WHERE p.investor_id = i.investor_id 
                        AND p.currently_owned = true
                        ORDER BY p.purchase_date DESC
                        LIMIT :property_limit
                    ) p
                ) as owned_properties,
                
                -- Previously owned properties (sold)
                (
                    SELECT json_agg(
                        json_build_object(
                            'property_id', p.property_id,
                            'address', p.situs_address,
                            'city', p.situs_city,
                            'state', p.situs_state,
                            'zip', p.situs_zip5,
                            'purchase_date', p.purchase_date,
                            'purchase_price', p.purchase_price,
                            'sale_date', p.sale_date,
                            'sale_price', p.sale_price,
                            'bedrooms', p.bedrooms,
                            'bathrooms', p.bathrooms,
                            'sqft', p.sqft,
                            'year_built', p.year_built,
                            'property_class', p.property_class,
                            'cash_purchase', p.cash_purchase,
                            'property_type', p.property_type,
                            'lot_size', p.lot_size,
                            'style_code', p.style_code,
                            'concurrent_mtg1_loan_amt', p.concurrent_mtg1_loan_amt,
                            'concurrent_mtg2_loan_amt', p.concurrent_mtg2_loan_amt,
                            'avm', p.avm,
                            'arv', p.arv,
                            'accuracy_score', p.accuracy_score,
                            'entity_name', p.entity_name,
                            'is_absentee', p.is_absentee,
                            'lat', ST_Y(p.location::geometry),
                            'lng', ST_X(p.location::geometry)
                        )
                    )
                    FROM (
                        SELECT property_id, situs_address, situs_city, situs_state, situs_zip5,
                               purchase_date, purchase_price, sale_date, sale_price,
                               bedrooms, bathrooms, sqft, year_built, property_class,
                               cash_purchase, is_absentee, location, property_type, lot_size, style_code, concurrent_mtg1_loan_amt, concurrent_mtg2_loan_amt, avm, arv, accuracy_score, entity_name
                        FROM investor_cache.properties p
                        WHERE p.investor_id = i.investor_id 
                        AND p.currently_owned = false
                        AND p.sale_date IS NOT NULL
                        ORDER BY p.sale_date DESC
                        LIMIT :property_limit
                    ) p
                ) as sold_properties,
                
                -- Market presence
                (
                    SELECT array_agg(DISTINCT ma.fips || ':' || ma.zip5)
                    FROM investor_cache.market_activity ma
                    WHERE ma.investor_id = i.investor_id
                ) as active_markets,
                -- Total AVM
                (
                    SELECT COALESCE(SUM(p.avm), 0)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.currently_owned = true
                ) as total_avm,
                -- Total mortgages
                (
                    SELECT COALESCE(SUM(
                        COALESCE(p.concurrent_mtg1_loan_amt, 0) + COALESCE(p.concurrent_mtg2_loan_amt, 0)
                    ), 0)
                    FROM investor_cache.properties p
                    WHERE p.investor_id = i.investor_id
                      AND p.currently_owned = true
                      AND (
                          (p.concurrent_mtg1_loan_amt IS NOT NULL AND p.concurrent_mtg1_loan_amt > 0)
                          OR
                          (p.concurrent_mtg2_loan_amt IS NOT NULL AND p.concurrent_mtg2_loan_amt > 0)
                      )
                ) as total_mortgages
            FROM investor_cache.investors i
            WHERE 
                CASE 
                    WHEN :search_type = 'investor_id' THEN i.investor_id = :investor_id_value
                    WHEN :search_type = 'mailing_key' THEN UPPER(i.mailing_key) = UPPER(:mailing_key_value)
                    WHEN :search_type = 'entity_name' THEN i.entity_names @> ARRAY[UPPER(:entity_names)]
                END
            ORDER BY i.activity_score DESC
            LIMIT :limit
        ";

        try {
            $stmt = $this->pgConn->prepare($sql);
            $stmt->bindParam(':search_type', $searchType);
            $stmt->bindParam(':property_limit', $property_limit, PDO::PARAM_INT);

            // Bind the appropriate parameter based on search type
            if ($searchType === 'investor_id') {
                $stmt->bindParam(':investor_id_value', $searchValue, PDO::PARAM_INT);
                $stmt->bindValue(':mailing_key_value', null, PDO::PARAM_NULL);
                $stmt->bindValue(':entity_names', null, PDO::PARAM_NULL);
            } elseif ($searchType === 'mailing_key') {
                $stmt->bindValue(':investor_id_value', null, PDO::PARAM_NULL);
                $stmt->bindParam(':mailing_key_value', $searchValue);
                $stmt->bindValue(':entity_names', null, PDO::PARAM_NULL);
            } elseif ($searchType === 'entity_name') {
                $stmt->bindValue(':investor_id_value', null, PDO::PARAM_NULL);
                $stmt->bindValue(':mailing_key_value', null, PDO::PARAM_NULL);
                $stmt->bindParam(':entity_names', $searchValue);
            }

            $stmt->bindParam(':limit', $limit, PDO::PARAM_INT);
            $stmt->execute();

            $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Format response
            $formattedResults = array_map(function ($row) {
                return [
                    'investor_id' => $row['investor_id'],
                    'primary_name' => $row['primary_name'],
                    'investor_identifier' => $row['mailing_key'],
                    'all_entity_names' => $this->parsePostgresArray($row['all_names']),
                    'is_corporate' => $row['is_corporate'],
                    'mailing_address' => [
                        'street' => $row['mailing_address'],
                        'city' => $row['mailing_city'],
                        'state' => $row['mailing_state'],
                        'zip' => $row['mailing_zip5']
                    ],
                    'portfolio_metrics' => [
                        'total_owned_properties' => $row['properties_owned_total'],
                        'total_sold_properties' => $row['properties_sold_total'],
                        'absentee_properties' => $row['properties_owned_absentee'],
                        'cash_purchases' => $row['properties_cash_purchased'],
                        'total_investment' => round($row['total_investment']),
                        'total_avm' => round($row['total_avm']),
                        'total_equity' => round($row['total_avm'] - $row['total_mortgages']),
                        'total_sold_value' => round($row['total_sold_value']),
                        'avg_property_value' => round($row['avg_property_value']),
                        'investor_type' => $row['investor_type'],
                        'activity_score' => $row['activity_score'],
                        'total_mortgages' => $row['total_mortgages']
                    ],
                    'activity' => [
                        'days_inactive' => $row['days_inactive'],
                        'last_activity' => $row['last_activity_date'],
                        'first_purchase_date' => $row['first_purchase_date'],
                        'last_purchase_date' => $row['last_purchase_date'],
                        'last_sale_date' => $row['last_sale_date']
                    ],
                    'properties' => [
                        'owned' => json_decode($row['owned_properties']) ?: [],
                        'sold' => json_decode($row['sold_properties']) ?: []
                    ],
                    'active_markets' => $row['active_markets']
                ];
            }, $results);

            APIHelper::SendResponse($formattedResults, count($formattedResults));

        } catch (PDOException $e) {
            APIHelper::SendResponse(null, 0, "Search failed: " . $e->getMessage());
        }
    }
}

$api = new FindInvestorAPI();
$api->search();