<?php

class APIHelper
{
    const HeaderAPIKey = 'Revamp365-123xyz';
    public static $filterPropertiesItems = [ // key = filter name, values column name and operator.
        'delta_min' => ['delta_psf', '>='],
        'delta_max' => ['delta_psf', '<='],
        'est_profit_min' => ['est_profit', '>='],
        'est_profit_max' => ['est_profit', '<='],
        'est_cashflow_min' => ['est_cashflow', '>='],
        'est_cashflow_max' => ['est_cashflow', '<='],
        'remarks_public_keywords' => ['remarks_public', 'ilike'],
        'city_name_keyword' => ['city_name', 'like'],
        'dom_min' => ['dom', '>='], // additional entry_date filter included
        'dom_max' => ['dom', '<='], // additional entry_date filter included
        'est_arv_min' => ['est_arv', '>='],
        'est_arv_max' => ['est_arv', '<='],
        'avm_min' => ['est_avm', '>='],
        'avm_max' => ['est_avm', '<='],
        'lot_sqf_min' => ['lot_sqft', '>='],
        'lot_sqf_max' => ['lot_sqft', '<='],
        'total_finished_sqft_min' => ['total_finished_sqft', '>='],
        'total_finished_sqft_max' => ['total_finished_sqft', '<='],
        'list_price_min' => ['list_price', '>='],
        'list_price_max' => ['list_price', '<='],
        'medianrent_min' => ['medianrent', '>='],
        'medianrent_max' => ['medianrent', '<='],
        'state_or_province_keyword' => ['state_or_province', '='],
        'bedrooms_min' => ['bedrooms_count', '>='],
        'bedrooms_max' => ['bedrooms_count', '<='],
        'bathrooms_min' => ['bathrooms_total_count', '>='],
        'bathrooms_max' => ['bathrooms_total_count', '<='],
        'year_built_min' => ['year_built', '>='],
        'year_built_max' => ['year_built', '<='],
        'county' => ['county', '='],
        'status' => ['status', '='],
        'list_agent_keyword' => ['list_agent_first_name', '='], // list_agent fields included
        'fulladdress_keyword' => ['geo_address', 'ilike'],
        'fulladdress_avoid' => ['geo_address', 'not ilike'],
        'structure_type' => ['structure_type', '='],
        'mls_number' => ['mls_number', '='],
        'id' => ['id', '='],
        'zoning' => ['zoning', '='],
        'listing_entry_date_min' => ['listing_entry_date', '>='],
        'listing_entry_date_max' => ['listing_entry_date', '<='],
        'closed_date_min' => ['close_date', '>='],
        'closed_date_max' => ['close_date', '<='],
        'zip' => ['zip_code', '='],
        'deal_type' => ['wholesale', '='],
        'filter_ids' => ['id', '='],
        'city_names_avoid' => ['city_name', '='],
        'school_district' => ['school_district_name', '='],
        //        'distance_max' => ['id', '='],
        'ids' => ['id', 'in'],
        'accuracy_score_value' => ['accuracy_score_value', '>='],
        'accuracy_score_rent' => ['accuracy_score_rent', '>='],
        'map_bound_range' => ['lat_lng', 'Between'],
        'suppress_ids' => ['id', '!='],
        // New fields.
        'suppress_city' => ['city_name', '!='], // Array
        'suppress_zip_code' => ['zip_code', '!='], // Array
        'active_days_min' => ['mls_modification_at', '<='],
        'active_days_max' => ['mls_modification_at', '>='],
        'active_under_contract_days_min' => ['mls_modification_at', '<='],
        'active_under_contract_days_max' => ['mls_modification_at', '>='],
        'canceled_days_min' => ['mls_modification_at', '<='],
        'canceled_days_max' => ['mls_modification_at', '>='],
        'closed_days_min' => ['mls_modification_at', '<='],
        'closed_days_max' => ['mls_modification_at', '>='],
        'coming_soon_days_min' => ['mls_modification_at', '<='],
        'coming_soon_days_max' => ['mls_modification_at', '>='],
        'expired_days_min' => ['mls_modification_at', '<='],
        'expired_days_max' => ['mls_modification_at', '>='],
        'pending_days_min' => ['mls_modification_at', '<='],
        'pending_days_max' => ['mls_modification_at', '>='],
        'temp_off_market_days_min' => ['mls_modification_at', '<='],
        'temp_off_market_days_max' => ['mls_modification_at', '>='],
        'withdrawn_days_min' => ['mls_modification_at', '>='],
        'withdrawn_days_max' => ['mls_modification_at', '<='],
        'cool_type' => ['cooling_type', '='], // Array
        'heat_type' => ['heating_type', '='], // Array
        'water_source' => ['water_source', '='], // Array
        'sewer_source' => ['sewer_septic', '='], // Array
        'hoa' => ['hoa_yn', '='], // Y or N or empty
        'hoa_amount_min' => ['hoa_fee', '>='],
        'hoa_amount_max' => ['hoa_fee', '<='],
    ];
    public static $orderByItems = [
        // Older
        'DOM (Longest to shortest)' => ['dom', 'DESC'],
        'DOM (Shortest to longest)' => ['dom', 'ASC'],
        'Delta PSF (High to low)' => ['delta_psf', 'DESC'],
        'Flip Profit (High to low)' => ['est_profit', 'DESC'],
        'Cash Flow (High to low)' => ['est_cashflow', 'DESC'],
        'Rent (High to low)' => ['medianrent', 'DESC'],
        'AVM (High to low)' => ['est_avm', 'DESC'],
        'List Price (High to low)' => ['list_price', 'DESC'],
        'List Price (Low to high)' => ['list_price', 'ASC'],
        'Modification Date' => ['modification_timestamp', 'DESC'],

        //New
        'modification_date_desc' => ['modification_timestamp', 'DESC'],
        'dom_desc' => ['dom', 'DESC'],
        'dom_asc' => ['dom', 'ASC'],
        'delta_psf_desc' => ['delta_psf', 'DESC'],
        'est_profit_desc' => ['est_profit', 'DESC'],
        'est_cashflow_desc' => ['est_cashflow', 'DESC'],
        'medianrent_desc' => ['medianrent', 'DESC'],
        'est_avm_desc' => ['est_avm', 'DESC'],
        'list_price_desc' => ['list_price', 'DESC'],
        'list_price_asc' => ['list_price', 'ASC'],
    ];

    public function __construct()
    {

    }

    public static function setAccuracyParams()
    {
        $accuracyParams = ['accuracy_score_rent', 'accuracy_score_value'];

        foreach ($accuracyParams as $param) {
            $value = self::GetParam($param, true);

            if (is_numeric($value) && $value >= 10) {
                $adjusted = $value / 10;

                if (isset($_REQUEST[$param])) {
                    $_REQUEST[$param] = $adjusted;
                }

                if (isset($_POST[$param])) {
                    $_POST[$param] = $adjusted;
                }

                if (isset($_GET[$param])) {
                    $_GET[$param] = $adjusted;
                }

                if (isset($_COOKIE[$param])) {
                    $_COOKIE[$param] = $adjusted;
                }
            }
        }
    }

    public static function RemoveParamIfZero()
    {
        $unsetParams = ['accuracy_score_rent', 'accuracy_score_value'];
        foreach ($unsetParams as $param) {
            if (self::GetParam($param, true) == 0) {
                if (isset($_REQUEST[$param])) {
                    unset($_REQUEST[$param]);
                }

                if (isset($_POST[$param])) {
                    unset($_POST[$param]);
                }

                if (isset($_GET[$param])) {
                    unset($_GET[$param]);
                }

                if (isset($_COOKIE[$param])) {
                    unset($_COOKIE[$param]);
                }
            }
        }
    }

    public static function Authentication($allJsonPayload = false)
    {

        if ($_SERVER['REQUEST_METHOD'] === 'POST') {
            $contentType = isset($_SERVER['CONTENT_TYPE']) ? strtolower($_SERVER['CONTENT_TYPE']) : '';
            // Handle JSON payload if $allJsonPayload is true
            if ($allJsonPayload && strpos($contentType, 'application/json') !== false) {
                // Read JSON from request body
                $jsonInput = file_get_contents('php://input');
                $jsonData = json_decode($jsonInput, true);
                
                if (json_last_error() !== JSON_ERROR_NONE) {
                    header('Content-Type: application/json', true, 400);
                    echo self::SendResponse([], '0', 'Invalid JSON payload: ' . json_last_error_msg());
                    exit;
                }
                
                // Populate superglobals with JSON data
                if (is_array($jsonData)) {
                    foreach ($jsonData as $key => $value) {
                        $_REQUEST[$key] = $value;
                        $_POST[$key] = $value;
                        // Also add to $_GET for compatibility
                        $_GET[$key] = $value;
                    }
                }
            } elseif (strpos($contentType, 'multipart/form-data') === false) {
                header('Content-Type: application/json', true, 400);
                echo self::SendResponse([], '0', 'Request must be multipart/form-data not ' . ($_SERVER['CONTENT_TYPE'] ?? 'custom type'));
                exit;
            }
        }

        $auth = (self::GetHeaderElements(['API_KEY', 'api_key', 'Api-Key','HTTP_API_KEY']) == self::HeaderAPIKey);

        if (!$auth) {
            echo self::SendResponse([], '0', 'Revamp API key must be presented');
            exit();
        }
        if (APIHelper::GetParam('status') && APIHelper::GetParam('status') == 'Closed') {
            $closedDate = APIHelper::GetParam('closed_date_min', true) ?? null;
            if (!$closedDate) {
                $now = new DateTime();
                $sixMonthsAgo = $now->modify('-6 months');
                $_REQUEST['closed_date_min'] = $sixMonthsAgo->format('Y-m-d');
                $_POST['closed_date_min'] = $sixMonthsAgo->format('Y-m-d');
            }
        }

        return true;
    }

    public static function GetHeaderElements($keys)
    {
        $header = getallheaders();
        foreach ($keys as $key) {
            if (isset($header[$key])) {
                return $header[$key];
            }
        }
        return null;
    }


    public static function GetParam($key = null, $trim = false)
    {

        if ($key) {
            return isset($_REQUEST[$key]) && trim($_REQUEST[$key]) != '' ? ($trim ? trim($_REQUEST[$key]) : $_REQUEST[$key]) : null;
        }
        return null;
    }

    public static function SendResponse($data, $status, $message = '')
    {
        if (ob_get_length())
            ob_clean();
        header('content-type: application/json');
        header('Cache-Control: no-store');
        echo json_encode(['status' => $status, 'message' => $message, 'data' => $data, 'count' => is_array($data) ? count($data) : 0], JSON_PARTIAL_OUTPUT_ON_ERROR | JSON_UNESCAPED_UNICODE);
        exit;
    }

    public static function GetPropertyFilterQuery(&$queryParams, $filterKey, $columnName, $operator = '=') // Filter property
    {
        $whereClause = '';
        if (APIHelper::GetParam($filterKey) != null) {
            $likeOperators = [
                'ilike',
                'not ilike',
                'like',
                'not like',
            ];

            // Param setting
            $filterValueCount = 1;
            if (in_array(trim(strtolower($operator)), $likeOperators)) { // Check if "like" condition or not.
                $queryParams[] = "%" . APIHelper::GetParam($filterKey) . "%";
            } elseif ($filterKey == 'listing_entry_date_min' || $filterKey == 'listing_entry_date_max' || $filterKey == 'closed_date_min' || $filterKey == 'closed_date_max') {// Date values
                $dateValue = @strtotime(APIHelper::GetParam($filterKey));
                $queryParams[] = @date('Y-m-d H:i:s', $dateValue);
            } elseif (
                $filterKey == 'status' || $filterKey == 'structure_type' || $filterKey == 'county'
                || $filterKey == 'state_or_province_keyword' || $filterKey == 'filter_ids'
                || $filterKey == 'suppress_ids' || $filterKey == 'city_names_avoid'
                || $filterKey == 'school_district' || $filterKey == 'zoning'
                || $filterKey == 'cool_type' || $filterKey == 'heat_type'
                || $filterKey == 'water_source' || $filterKey == 'sewer_source'
                || $filterKey == 'suppress_city' || $filterKey == 'suppress_zip_code'
            ) {
                // Check if multi values condition.
                $completeValues = GetFilteredCommaSepValues(strtolower(APIHelper::GetParam($filterKey)));
                $queryParams[] = '{' . $completeValues . '}';
            } elseif ($filterKey == 'zip') {
                $queryParams[] = @ltrim(APIHelper::GetParam($filterKey), '0');
            } elseif ($filterKey == 'map_bound_range') {
                // This should be empty, will be take cared by value matching.
            } elseif ($filterKey == 'active_days_min' || $filterKey == 'active_days_max' || $filterKey == 'active_under_contract_days_min' || $filterKey == 'active_under_contract_days_max' || $filterKey == 'canceled_days_min' || $filterKey == 'canceled_days_max' || $filterKey == 'closed_days_min' || $filterKey == 'closed_days_max' || $filterKey == 'coming_soon_days_min' || $filterKey == 'coming_soon_days_max' || $filterKey == 'expired_days_min' || $filterKey == 'expired_days_max' || $filterKey == 'pending_days_min' || $filterKey == 'pending_days_max' || $filterKey == 'temp_off_market_days_min' || $filterKey == 'temp_off_market_days_max' || $filterKey == 'withdrawn_days_min' || $filterKey == 'withdrawn_days_max') {
                //                preg_match('/^(active|active_under_contract|canceled|closed|coming_soon|expired|pending|temp_off_market|withdrawn)_days_(min|max)$/', $filterKey, $matches);
//                $status = self::StatusMap($matches[1]);
//                $primeStates = [];
//                if ($st = APIHelper::GetParam('status')) {
//                    $st2 = explode(',', $st);
//                    $primeStates = array_filter(array_map('trim', $st2));
//                }
//                if (in_array($status, $primeStates)) {
//                    $queryParams [] = APIHelper::GetParam($filterKey);
//                }
            } else {
                $queryParams[] = APIHelper::GetParam($filterKey);
            }

            // Value Matching
            if ($filterKey == 'dom_max' || $filterKey == 'dom_min') {
                $whereClause = " AND EXTRACT(DAY FROM (NOW() - listing_entry_date)) $operator $" . count($queryParams) . " ";
            } elseif (
                $filterKey == 'structure_type' || $filterKey == 'county'
                || $filterKey == 'state_or_province_keyword' || $filterKey == 'status'
                || $filterKey == 'school_district' || $filterKey == 'zoning'
                || $filterKey == 'cool_type' || $filterKey == 'heat_type'
                || $filterKey == 'water_source' || $filterKey == 'sewer_source'
            ) {
                $whereClause = " AND LOWER($columnName) $operator ANY($" . count($queryParams) . ") ";
            } elseif ($filterKey == 'city_names_avoid') {
                $whereClause = " AND NOT (LOWER($columnName) $operator ANY($" . count($queryParams) . ")) ";
            } elseif ($filterKey == 'closed_date_min' || $filterKey == 'closed_date_max') {
                $whereClause = " AND (
                    CASE 
                        WHEN status = 'Closed' THEN $columnName $operator $" . count($queryParams) . "
                        ELSE TRUE
                    END
                )";
            } elseif ($filterKey == 'filter_ids') {
                $whereClause = " AND $columnName $operator ANY($" . count($queryParams) . ") ";
            } elseif ($filterKey == 'suppress_ids' || $filterKey == 'suppress_city' || $filterKey == 'suppress_zip_code') {
                $whereClause = " AND $columnName != ALL($" . count($queryParams) . ") ";
            } elseif ($filterKey == 'map_bound_range') {
                $_area = @json_decode(APIHelper::GetParam($filterKey), true);
                $whereClause = " AND latitude BETWEEN " . ($_area[0]) . " AND " . ($_area[1]) . " AND longitude BETWEEN " . ($_area[2]) . " AND " . ($_area[3]) . " ";
            } elseif ($filterKey == 'list_agent_keyword') {
                $whereClause = " AND TRIM(LOWER(CONCAT(list_agent_first_name, ' ', list_agent_last_name))) = TRIM(LOWER($" . count($queryParams) . ")) ";
            } elseif (
                $filterKey == 'active_days_min' || $filterKey == 'active_days_max' || $filterKey == 'active_under_contract_days_min' || $filterKey == 'active_under_contract_days_max' || $filterKey == 'canceled_days_min' || $filterKey == 'canceled_days_max' || $filterKey == 'closed_days_min' || $filterKey == 'closed_days_max' || $filterKey == 'coming_soon_days_min' || $filterKey == 'coming_soon_days_max' || $filterKey == 'expired_days_min' || $filterKey == 'expired_days_max' || $filterKey == 'pending_days_min' || $filterKey == 'pending_days_max' || $filterKey == 'temp_off_market_days_min' || $filterKey == 'temp_off_market_days_max' || $filterKey == 'withdrawn_days_min' || $filterKey == 'withdrawn_days_max'
            ) {
                preg_match('/^(active|active_under_contract|canceled|closed|coming_soon|expired|pending|temp_off_market|withdrawn)_days_(min|max)$/', $filterKey, $matches);
                $status = self::StatusMap($matches[1]);
                $primeStates = [];
                if ($st = APIHelper::GetParam('status')) {
                    $st2 = explode(',', $st);
                    $primeStates = array_filter(array_map('trim', $st2));
                }
                if (in_array($status, $primeStates) && APIHelper::GetParam($filterKey)) {
                    $queryParams[] = APIHelper::GetParam($filterKey);
                    $whereClause = " AND (
                            status != '$status' 
                            OR mls_modification_at $operator (NOW() - ($" . count($queryParams) . " || ' days')::INTERVAL)
                        )";
                }

            } else {
                $whereClause = " AND $columnName $operator $" . count($queryParams) . " ";
            }
        }

        return $whereClause;
    }

    private static function StatusMap($var)
    {
        switch (strtolower($var)) {
            case 'active':
                return 'Active';
            case 'active_under_contract':
                return 'Active Under Contract';
            case 'canceled':
                return 'Canceled';
            case 'closed':
                return 'Closed';
            case 'coming_soon':
                return 'Coming Soon';
            case 'expired':
                return 'Expired';
            case 'pending':
                return 'Pending';
            case 'temp_off_market':
                return 'Temp Off Market';
            case 'withdrawn':
                return 'Withdrawn';
            default:
                return ucfirst($var); // fallback
        }
    }

    public static function GetPropertySortBy(): string
    {
        $orderByQuery = ' random() ';
        if (self::GetParam('order_by') != null) {
            $orderBy = trim(self::GetParam('order_by'));
            if (isset(self::$orderByItems[$orderBy])) {
                $orderByItem = self::$orderByItems[$orderBy];
                if ($orderByItem[0] != 'dom') {
                    $orderByQuery = " " . $orderByItem[0] . " " . $orderByItem[1] . " ";
                } else {
                    $orderByQuery = " EXTRACT(DAY FROM (NOW() - listing_entry_date)) " . $orderByItem[1] . " ";
                }
            }
        }
        return $orderByQuery;
    }

    public static function initialPropertyWhereClause()
    {
        $wholesaleRestriction = " AND (
        wholesale <> 'Wholesale'
        OR (wholesale = 'Wholesale' AND status = 'Active')
      ) "; // Wholesale should be Active.

        $whereClause = ' WHERE 1 = 1 AND is_calculated = true AND bubble_sync = true ';

        return $whereClause . $wholesaleRestriction;
    }
}


function calculatePercentageChange($previous, $current)
{
    if ($previous == 0) {
        if ($current == 0) {
            return 0;
        } else {
            return 100;
        }
    }
    return round((($current - $previous) / $previous) * 100, 2);
}

function getPreviousDaysRange($rangeDays)
{
    $rangeDays = (!$rangeDays) ? 30 : $rangeDays;
    $rangeStart = $rangeDays;
    $rangeEnd = $rangeDays + 30;
    return ['start' => $rangeStart . ' days', 'end' => $rangeEnd . " days"];
}


function normalizeStreetSuffix($address)
{
    $replacements = [
        '/\broad\b/i' => '{rd}',
        '/\bavenue\b/i' => '{ave}',
        '/\bstreet\b/i' => '{st}',
        '/\bboulevard\b/i' => '{blvd}',
        '/\blane\b/i' => '{ln}',
        '/\bdrive\b/i' => '{dr}',
        // Short forms last (only if standalone, not within {})
        '/\brd\b(?!\})/i' => '{rd}',
        '/\bave\b(?!\})/i' => '{ave}',
        '/\bst\b(?!\})/i' => '{st}',
        '/\bblvd\b(?!\})/i' => '{blvd}',
        '/\bln\b(?!\})/i' => '{ln}',
        '/\bdr\b(?!\})/i' => '{dr}',
    ];
    return preg_replace(array_keys($replacements), array_values($replacements), strtolower($address));
}

function normalizeQueryWhereClause()
{
    return " REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(lower(SitusFullStreetAddress), 'road', '{rd}'),
                            'rd', '{rd}'),
                        'avenue', '{ave}'),
                    'ave', '{ave}'),
                'street', '{st}'),
            'st', '{st}'),
        'boulevard', '{blvd}'),
    'blvd', '{blvd}')
    LIKE CONCAT('%', :normalized_address, '%') ";
}