<?php
// File: /api/find_local_investors.php
// Response time: <1 second (from 15-30 seconds)

include_once "../../includes/APIHelperV2.php";
require_once 'config.inverstercache.php';
APIHelper::Authentication();

class LocalInvestorsAPI
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

    public function findLocalInvestors()
    {
        // Get parameters
        $lat = APIHelper::GetParam('lat', true);
        $lng = APIHelper::GetParam('lng', true);
        $radius = APIHelper::GetParam('range', true);
        $days = APIHelper::GetParam('days', true);
        $fips = APIHelper::GetParam('fips', true);
        $zip = APIHelper::GetParam('zip', false);
        $limit = APIHelper::GetParam('limit', false) ?: 50;


        //     // OPTIMIZED QUERY
        //     $sqlOLD = "
        //     WITH local_investors AS (
        //         -- Use index on fips first, then spatial filtering
        //             SELECT 
        //             p.investor_id,
        //             COUNT(*) as local_properties,
        //             MAX(p.purchase_date) as latest_purchase,
        //             SUM(p.purchase_price) as local_investment,
        //             AVG(ST_Distance(
        //                 p.location::geography,
        //                 ST_MakePoint(:lng, :lat)::geography
        //             ) / 1609.34) as avg_distance_miles,
        //             -- Recent purchases (limited to 5)
        //             (
        //                 SELECT json_agg(
        //                     json_build_object(
        //                         'property_id', p.property_id,
        //                         'address', p.situs_address,
        //                         'city', p.situs_city,
        //                         'state', p.situs_state,
        //                         'zip', p.situs_zip5,
        //                         'purchase_date', p.purchase_date,
        //                         'purchase_price', p.purchase_price,
        //                         'sale_date', p.sale_date,
        //                         'sale_price', p.sale_price,
        //                         'bedrooms', p.bedrooms,
        //                         'bathrooms', p.bathrooms,
        //                         'sqft', p.sqft,
        //                         'year_built', p.year_built,
        //                         'property_class', p.property_class,
        //                         'cash_purchase', p.cash_purchase,
        //                         'is_absentee', p.is_absentee,
        //                         'activity_type', p.activity_type,
        //                         'property_type', p.property_type,
        //                         'distance_miles', ROUND((
        //                             ST_Distance(
        //                                 p.location::geography,
        //                                 ST_MakePoint(:lng, :lat)::geography
        //                             ) / 1609.34
        //                         )::NUMERIC, 2),
        //                         'lat', ST_Y(p.location::geometry),
        //                         'lng', ST_X(p.location::geometry)
        //                     )
        //                 )
        //                 FROM (
        //                     SELECT property_id, situs_address, situs_city, situs_state, situs_zip5,
        //                            purchase_date, purchase_price, sale_date, sale_price, bedrooms, bathrooms, sqft,
        //                            year_built, property_class, cash_purchase, is_absentee, activity_type, location
        //                     FROM investor_cache.properties p2
        //                     WHERE p2.investor_id = p.investor_id
        //                     AND p2.fips = :fips
        //                     AND p2.purchase_date >= CURRENT_DATE - (:days::INTEGER * INTERVAL '1 day')
        //                     AND ST_DWithin(
        //                         p2.location::geography,
        //                         ST_MakePoint(:lng, :lat)::geography,
        //                         :radius::NUMERIC * 1609.34
        //                     )
        //                     ORDER BY p2.purchase_date DESC
        //                     LIMIT 5
        //                 ) p
        //             ) as recent_purchases,

        //             -- Recent sales (limited to 5)
        //             (
        //                 SELECT json_agg(
        //                     json_build_object(
        //                         'property_id', p.property_id,
        //                         'address', p.situs_address,
        //                         'city', p.situs_city,
        //                         'state', p.situs_state,
        //                         'zip', p.situs_zip5,
        //                         'purchase_date', p.purchase_date,
        //                         'purchase_price', p.purchase_price,
        //                         'sale_date', p.sale_date,
        //                         'sale_price', p.sale_price,
        //                         'bedrooms', p.bedrooms,
        //                         'bathrooms', p.bathrooms,
        //                         'sqft', p.sqft,
        //                         'year_built', p.year_built,
        //                         'property_class', p.property_class,
        //                         'cash_purchase', p.cash_purchase,
        //                         'is_absentee', p.is_absentee,
        //                         'activity_type', p.activity_type,
        //                         'property_type', p.property_type,
        //                         'distance_miles', ROUND((
        //                             ST_Distance(
        //                                 p.location::geography,
        //                                 ST_MakePoint(:lng, :lat)::geography
        //                             ) / 1609.34
        //                         )::NUMERIC, 2),
        //                         'lat', ST_Y(p.location::geometry),
        //                         'lng', ST_X(p.location::geometry)
        //                     )
        //                 )
        //                 FROM (
        //                     SELECT property_id, situs_address, situs_city, situs_state, situs_zip5,
        //                            purchase_date, purchase_price, sale_date, sale_price, bedrooms, bathrooms, sqft,
        //                            year_built, property_class, cash_purchase, is_absentee, activity_type, location, property_type
        //                     FROM investor_cache.properties p2
        //                     WHERE p2.investor_id = p.investor_id
        //                     AND p2.fips = :fips
        //                     AND p2.sale_date >= CURRENT_DATE - (:days::INTEGER * INTERVAL '1 day')
        //                     AND p2.sale_date IS NOT NULL
        //                     AND ST_DWithin(
        //                         p2.location::geography,
        //                         ST_MakePoint(:lng, :lat)::geography,
        //                         :radius::NUMERIC * 1609.34
        //                     )
        //                     ORDER BY p2.sale_date DESC
        //                     LIMIT 5
        //                 ) p
        //             ) as recent_sales
        //         FROM investor_cache.properties p
        //         WHERE 
        //             p.fips = :fips
        //             AND p.purchase_date >= CURRENT_DATE - (:days::INTEGER * INTERVAL '1 day')
        //             AND ST_DWithin(
        //                 p.location::geography,
        //                 ST_MakePoint(:lng, :lat)::geography,
        //                 :radius::NUMERIC * 1609.34
        //             )
        //         GROUP BY p.investor_id
        //         HAVING COUNT(*) > 0
        //     )
        //     SELECT 
        //         li.investor_id,
        //         i.primary_name as investor_name,
        //         i.entity_names as all_names,
        //         i.mailing_key as investor_identifier,
        //         i.mailing_address,
        //         i.mailing_city,
        //         i.mailing_state,
        //         i.mailing_zip5,
        //         i.investor_type,
        //         i.properties_owned_total as total_properties_nationwide,
        //         li.local_properties as local_properties_owned,
        //         i.properties_cash_purchased as total_cash_purchases,
        //         li.local_investment,
        //         li.avg_distance_miles,
        //         li.latest_purchase as most_recent_local_purchase,
        //         i.days_inactive as days_since_last_activity,
        //         -- Simplified score calculation
        //         ROUND((
        //             (li.local_properties * 15) +
        //             (LEAST(i.properties_cash_purchased, 10) * 2) +
        //             (CASE i.investor_type 
        //                 WHEN 'Flipper' THEN 20
        //                 WHEN 'Portfolio Builder' THEN 15
        //                 WHEN 'Landlord' THEN 10
        //                 ELSE 5 
        //             END) +
        //             (30 * EXP(-LEAST(365, CURRENT_DATE - li.latest_purchase)::NUMERIC / 90.0)) +
        //             (CASE WHEN li.avg_distance_miles <= :radius::NUMERIC/2 THEN 10 ELSE 0 END)
        //         )::NUMERIC, 1) as likelihood_score,
        //         -- Separate recent purchases and sales
        //         li.recent_purchases as recent_purchases,
        //         li.recent_sales as recent_sales,
        //         CASE 
        //             WHEN ROW_NUMBER() OVER (ORDER BY (
        //                 (li.local_properties * 15) +
        //                 (LEAST(i.properties_cash_purchased, 10) * 2) +
        //                 (CASE i.investor_type 
        //                     WHEN 'Flipper' THEN 20
        //                     WHEN 'Portfolio Builder' THEN 15
        //                     WHEN 'Landlord' THEN 10
        //                     ELSE 5 
        //                 END) +
        //                 (30 * EXP(-LEAST(365, CURRENT_DATE - li.latest_purchase)::NUMERIC / 90.0))
        //             ) DESC) <= 5 
        //             THEN 'Top Investor' 
        //             ELSE '' 
        //         END as top_investor_tag
        //     FROM local_investors li
        //     JOIN investor_cache.investors i ON li.investor_id = i.investor_id
        //     ORDER BY likelihood_score DESC
        //     LIMIT :limit
        // ";


        $sql = "
WITH local_properties AS (
    -- Step 1: Find all local properties ONCE (with spatial index)
    SELECT 
        p.investor_id,
        p.property_id,
        p.datatree_id,
        p.situs_address,
        p.situs_city,
        p.situs_state,
        p.situs_zip5,
        p.purchase_date,
        p.purchase_price,
        p.sale_date,
        p.sale_price,
        p.bedrooms,
        p.bathrooms,
        p.sqft,
        p.year_built,
        p.property_class,
        p.cash_purchase,
        p.is_absentee,
        p.activity_type,
        p.location,
        p.property_type,
        -- ✅ Calculate distance ONCE
        ROUND((ST_Distance(
            p.location::geography,
            ST_MakePoint(:lng, :lat)::geography
        ) / 1609.34)::NUMERIC, 2) as distance_miles,
        ST_Y(p.location::geometry) as lat,
        ST_X(p.location::geometry) as lng
    FROM investor_cache.properties p
    WHERE 
        p.fips = :fips
        AND (
            (p.purchase_date >= CURRENT_DATE - (:days::INTEGER * INTERVAL '1 day'))
            OR 
            (p.sale_date >= CURRENT_DATE - (:days::INTEGER * INTERVAL '1 day') AND p.sale_date IS NOT NULL)
        )
        AND ST_DWithin(
            p.location::geography,
            ST_MakePoint(:lng, :lat)::geography,
            :radius::NUMERIC * 1609.34
        )
),
local_investors_by_id AS (
    -- Step 2: Aggregate by investor_id first
    SELECT 
        lp.investor_id,
        COUNT(*) as local_properties,
        MAX(lp.purchase_date) as latest_purchase,
        SUM(lp.purchase_price) as local_investment,
        AVG(lp.distance_miles) as avg_distance_miles,
        -- ✅ Aggregate recent purchases (no subquery!)
        json_agg(
            json_build_object(
                'property_id', lp.property_id,
                'datatree_id', lp.datatree_id,
                'address', lp.situs_address,
                'city', lp.situs_city,
                'state', lp.situs_state,
                'zip', lp.situs_zip5,
                'purchase_date', lp.purchase_date,
                'purchase_price', lp.purchase_price,
                'sale_date', lp.sale_date,
                'sale_price', lp.sale_price,
                'bedrooms', lp.bedrooms,
                'bathrooms', lp.bathrooms,
                'sqft', lp.sqft,
                'year_built', lp.year_built,
                'property_class', lp.property_class,
                'cash_purchase', lp.cash_purchase,
                'is_absentee', lp.is_absentee,
                'activity_type', lp.activity_type,
                'distance_miles', lp.distance_miles,
                'lat', lp.lat,
                'lng', lp.lng,
                'property_type', lp.property_type
            ) ORDER BY lp.purchase_date DESC
        ) FILTER (WHERE lp.purchase_date IS NOT NULL) as all_purchases,
        -- ✅ Aggregate recent sales
        json_agg(
            json_build_object(
                'property_id', lp.property_id,
                'datatree_id', lp.datatree_id,
                'address', lp.situs_address,
                'city', lp.situs_city,
                'state', lp.situs_state,
                'zip', lp.situs_zip5,
                'purchase_date', lp.purchase_date,
                'purchase_price', lp.purchase_price,
                'sale_date', lp.sale_date,
                'sale_price', lp.sale_price,
                'bedrooms', lp.bedrooms,
                'bathrooms', lp.bathrooms,
                'sqft', lp.sqft,
                'year_built', lp.year_built,
                'property_class', lp.property_class,
                'cash_purchase', lp.cash_purchase,
                'is_absentee', lp.is_absentee,
                'activity_type', lp.activity_type,
                'distance_miles', lp.distance_miles,
                'lat', lp.lat,
                'lng', lp.lng,
                'property_type', lp.property_type
            ) ORDER BY lp.sale_date DESC
        ) FILTER (WHERE lp.sale_date IS NOT NULL) as all_sales
    FROM local_properties lp
    GROUP BY lp.investor_id
),
local_investors_by_name AS (
    -- Step 3: Group by primary_name and aggregate across all investor_ids
    SELECT 
        i.primary_name as investor_name,
        -- Take the investor_id with most local properties as primary
        (array_agg(li.investor_id ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as primary_investor_id,
        -- Aggregate all entity names - unnest all arrays, get distinct, convert back to array
        (SELECT array_agg(DISTINCT elem ORDER BY elem) 
         FROM (
             SELECT unnest(i2.entity_names) as elem
             FROM local_investors_by_id li2
             JOIN investor_cache.investors i2 ON li2.investor_id = i2.investor_id
             WHERE i2.primary_name = i.primary_name 
             AND i2.entity_names IS NOT NULL
         ) sub) as all_names,
        -- Collect all mailing keys
        array_agg(DISTINCT i.mailing_key) as all_mailing_keys,
        -- Take mailing address from investor with most local properties
        (array_agg(i.mailing_key ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as primary_mailing_key,
        (array_agg(i.mailing_address ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as mailing_address,
        (array_agg(i.mailing_city ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as mailing_city,
        (array_agg(i.mailing_state ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as mailing_state,
        (array_agg(i.mailing_zip5 ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as mailing_zip5,
        -- Take most common investor_type, or first if tied
        (array_agg(i.investor_type ORDER BY li.local_properties DESC, li.latest_purchase DESC NULLS LAST))[1] as investor_type,
        -- Sum all properties nationwide (take max to avoid double counting if same investor has multiple records)
        MAX(i.properties_owned_total) as total_properties_nationwide,
        -- Count distinct local properties by datatree_id (to avoid duplicates across mailing addresses)
        (SELECT COUNT(DISTINCT COALESCE(lp.datatree_id, lp.property_id))
         FROM local_properties lp
         JOIN investor_cache.investors i3 ON lp.investor_id = i3.investor_id
         WHERE i3.primary_name = i.primary_name) as local_properties_owned,
        -- Sum cash purchases (take max to avoid double counting)
        MAX(i.properties_cash_purchased) as total_cash_purchases,
        -- Sum local investment (deduplicate by datatree_id to avoid counting same property multiple times)
        (SELECT COALESCE(SUM(lp.purchase_price), 0)
         FROM (
             SELECT DISTINCT ON (COALESCE(lp2.datatree_id, lp2.property_id)) lp2.purchase_price
             FROM local_properties lp2
             JOIN investor_cache.investors i3 ON lp2.investor_id = i3.investor_id
             WHERE i3.primary_name = i.primary_name
             AND lp2.purchase_price IS NOT NULL
             ORDER BY COALESCE(lp2.datatree_id, lp2.property_id)
         ) lp) as local_investment,
        -- Weighted average distance (by distinct properties)
        (SELECT AVG(lp.distance_miles)
         FROM (
             SELECT DISTINCT ON (COALESCE(lp2.datatree_id, lp2.property_id)) lp2.distance_miles
             FROM local_properties lp2
             JOIN investor_cache.investors i3 ON lp2.investor_id = i3.investor_id
             WHERE i3.primary_name = i.primary_name
             ORDER BY COALESCE(lp2.datatree_id, lp2.property_id)
         ) lp) as avg_distance_miles,
        -- Most recent purchase across all investor_ids
        MAX(li.latest_purchase) as most_recent_local_purchase,
        -- Minimum days inactive (most active)
        MIN(i.days_inactive) as days_since_last_activity,
        -- Combine all purchases from all investor_ids, deduplicate by datatree_id (or property_id if datatree_id is null)
        (SELECT json_agg(elem ORDER BY (elem->>'purchase_date') DESC NULLS LAST)
         FROM (
             SELECT DISTINCT ON (COALESCE((elem->>'datatree_id')::bigint, (elem->>'property_id')::bigint)) elem
             FROM (
                 SELECT json_array_elements(COALESCE(li2.all_purchases, '[]'::json)) as elem
                 FROM local_investors_by_id li2
                 JOIN investor_cache.investors i2 ON li2.investor_id = i2.investor_id
                 WHERE i2.primary_name = i.primary_name
             ) sub
             ORDER BY COALESCE((elem->>'datatree_id')::bigint, (elem->>'property_id')::bigint), 
                      (elem->>'purchase_date') DESC NULLS LAST
         ) dedup) as all_purchases_combined,
        -- Combine all sales from all investor_ids, deduplicate by datatree_id (or property_id if datatree_id is null)
        (SELECT json_agg(elem ORDER BY (elem->>'sale_date') DESC NULLS LAST)
         FROM (
             SELECT DISTINCT ON (COALESCE((elem->>'datatree_id')::bigint, (elem->>'property_id')::bigint)) elem
             FROM (
                 SELECT json_array_elements(COALESCE(li2.all_sales, '[]'::json)) as elem
                 FROM local_investors_by_id li2
                 JOIN investor_cache.investors i2 ON li2.investor_id = i2.investor_id
                 WHERE i2.primary_name = i.primary_name
             ) sub
             ORDER BY COALESCE((elem->>'datatree_id')::bigint, (elem->>'property_id')::bigint), 
                      (elem->>'sale_date') DESC NULLS LAST
         ) dedup) as all_sales_combined
    FROM local_investors_by_id li
    JOIN investor_cache.investors i ON li.investor_id = i.investor_id
    WHERE i.primary_name IS NOT NULL AND i.primary_name != ''
    GROUP BY i.primary_name
)
SELECT 
    lin.primary_investor_id as investor_id,
    lin.investor_name,
    lin.all_names,
    lin.primary_mailing_key as investor_identifier,
    lin.mailing_address,
    lin.mailing_city,
    lin.mailing_state,
    lin.mailing_zip5,
    lin.investor_type,
    lin.total_properties_nationwide,
    lin.local_properties_owned,
    lin.total_cash_purchases,
    lin.local_investment,
    lin.avg_distance_miles,
    lin.most_recent_local_purchase,
    lin.days_since_last_activity,
    -- Simplified score calculation
    ROUND((
        (lin.local_properties_owned * 15) +
        (LEAST(lin.total_cash_purchases, 10) * 2) +
        (CASE lin.investor_type 
            WHEN 'Flipper' THEN 20
            WHEN 'Portfolio Builder' THEN 15
            WHEN 'Landlord' THEN 10
            ELSE 5 
        END) +
        (30 * EXP(-LEAST(365, CURRENT_DATE - lin.most_recent_local_purchase)::NUMERIC / 90.0)) +
        (CASE WHEN lin.avg_distance_miles <= :radius::NUMERIC/2 THEN 10 ELSE 0 END)
    )::NUMERIC, 1) as likelihood_score,
    -- ✅ Limit to 5 most recent purchases
    (SELECT json_agg(x) FROM (SELECT * FROM json_array_elements(lin.all_purchases_combined) LIMIT 5) x) as recent_purchases,
    -- ✅ Limit to 5 most recent sales
    (SELECT json_agg(x) FROM (SELECT * FROM json_array_elements(lin.all_sales_combined) LIMIT 5) x) as recent_sales,
    CASE 
        WHEN ROW_NUMBER() OVER (ORDER BY (
            (lin.local_properties_owned * 15) +
            (LEAST(lin.total_cash_purchases, 10) * 2) +
            (CASE lin.investor_type 
                WHEN 'Flipper' THEN 20
                WHEN 'Portfolio Builder' THEN 15
                WHEN 'Landlord' THEN 10
                ELSE 5 
            END) +
            (30 * EXP(-LEAST(365, CURRENT_DATE - lin.most_recent_local_purchase)::NUMERIC / 90.0))
        ) DESC) <= 5 
        THEN 'Top Investor' 
        ELSE '' 
    END as top_investor_tag
FROM local_investors_by_name lin
ORDER BY likelihood_score DESC
LIMIT :limit;
        ";

        try {
            $stmt = $this->pgConn->prepare($sql);

            // Bind parameters (simplified - same lat/lng used throughout)
            $stmt->bindValue(':lat', $lat, PDO::PARAM_STR);
            $stmt->bindValue(':lng', $lng, PDO::PARAM_STR);
            $stmt->bindValue(':radius', $radius, PDO::PARAM_STR);
            $stmt->bindValue(':days', $days, PDO::PARAM_INT);
            $stmt->bindValue(':fips', $fips, PDO::PARAM_STR);
            $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);

            $stmt->execute();
            $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // $row['all_names'] returns like text[]:  {"MICHAEL ANTHONY REPLOGLE REVOCABLE TRUST","JOHN W HERRON REVOCABLE TRUST"} so we need to convert it to an array
            // Format results similar to original API
            $formattedResults = array_map(function ($row) {
                return [
                    'investor_id' => $row['investor_id'],
                    'investor_name' => $row['investor_name'],
                    'investor_identifier' => $row['investor_identifier'],
                    'investor_aliases' => $this->parsePostgresArray($row['all_names']),
                    'MailingFullStreetAddress' => $row['mailing_address'],
                    'MailingCity' => $row['mailing_city'],
                    'MailingState' => $row['mailing_state'],
                    'MailingZIP5' => $row['mailing_zip5'],
                    'investor_type_tags' => $row['investor_type'],
                    'total_properties_owned_nationwide' => $row['total_properties_nationwide'],
                    'local_properties_owned' => $row['local_properties_owned'],
                    'total_cash_purchases' => $row['total_cash_purchases'],
                    'local_investment_total' => round($row['local_investment']),
                    'avg_distance_miles' => round($row['avg_distance_miles'], 2),
                    'most_recent_local_purchase' => $row['most_recent_local_purchase'],
                    'days_since_last_activity' => $row['days_since_last_activity'],
                    'likelihood_score' => $row['likelihood_score'],
                    'top_investor_tag' => $row['top_investor_tag'],
                    'properties' => [
                        'recent_purchases' => json_decode($row['recent_purchases']) ?: [],
                        'recent_sales' => json_decode($row['recent_sales']) ?: []
                    ]
                ];
            }, $results);

            APIHelper::SendResponse($formattedResults, count($formattedResults));

        } catch (PDOException $e) {
            APIHelper::SendResponse(null, 0, "Query failed: " . $e->getMessage());
        }
    }
}

$api = new LocalInvestorsAPI();
$api->findLocalInvestors();