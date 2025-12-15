<?php
/**
 * Server Test Script
 * Tests: File accessibility, PHP functionality, MySQL and PostgreSQL connections
 * Access via: http://your-domain/buyers/debug/server-test.php
 */

header('Content-Type: text/html; charset=utf-8');

// Start output buffering for clean HTML
ob_start();

$results = [];
$allPassed = true;

// Test 1: File Accessibility & PHP Working
$results['file_accessible'] = [
    'name' => 'File Accessibility',
    'status' => 'PASS',
    'message' => 'File is accessible and PHP is executing',
    'details' => [
        'File Path' => __FILE__,
        'Server Time' => date('Y-m-d H:i:s'),
        'PHP SAPI' => php_sapi_name()
    ]
];

// Test 2: PHP Version & Extensions
$phpVersion = phpversion();
$phpMajorVersion = (int)explode('.', $phpVersion)[0];
$phpStatus = ($phpMajorVersion >= 7) ? 'PASS' : 'WARN';
if ($phpMajorVersion < 7) {
    $allPassed = false;
}

$requiredExtensions = ['pdo', 'pdo_mysql', 'pdo_pgsql'];
$loadedExtensions = [];
$missingExtensions = [];

foreach ($requiredExtensions as $ext) {
    if (extension_loaded($ext)) {
        $loadedExtensions[] = $ext;
    } else {
        $missingExtensions[] = $ext;
    }
}

$results['php_info'] = [
    'name' => 'PHP Version & Extensions',
    'status' => (empty($missingExtensions)) ? $phpStatus : 'FAIL',
    'message' => 'PHP ' . $phpVersion . ' is running',
    'details' => [
        'PHP Version' => $phpVersion,
        'Loaded Extensions' => !empty($loadedExtensions) ? implode(', ', $loadedExtensions) : 'None',
        'Missing Extensions' => !empty($missingExtensions) ? implode(', ', $missingExtensions) : 'None',
        'Memory Limit' => ini_get('memory_limit'),
        'Max Execution Time' => ini_get('max_execution_time') . ' seconds'
    ]
];

if (!empty($missingExtensions)) {
    $allPassed = false;
}

// Test 3: MySQL Connection
// Load config from existing config files (no hardcoded credentials)
$mysqlConfig = null;
$pgConfig = null;
$pgConfigPath = __DIR__ . '/../investor-caching/config.inverstercache.php';
$mysqlConfigPath = __DIR__ . '/../mysql_conn.php';

// Load PostgreSQL config file first (contains both MySQL and PostgreSQL constants)
if (file_exists($pgConfigPath)) {
    require_once $pgConfigPath;
    // PostgreSQL config file also defines MySQL constants
    if (defined('MYSQL_HOST') && defined('MYSQL_DB') && defined('MYSQL_USER') && defined('MYSQL_PASS')) {
        $mysqlConfig = [
            'host' => MYSQL_HOST,
            'dbname' => MYSQL_DB,
            'username' => MYSQL_USER,
            'password' => MYSQL_PASS
        ];
    }
    // PostgreSQL config
    if (defined('PG_HOST') && defined('PG_USER') && defined('PG_PASS') && defined('PG_DB') && defined('PG_PORT')) {
        $pgConfig = [
            'host' => PG_HOST,
            'port' => PG_PORT,
            'dbname' => PG_DB,
            'username' => PG_USER,
            'password' => PG_PASS
        ];
    }
}

// Fallback: Try mysql_conn.php if MySQL constants not found
if (!$mysqlConfig && file_exists($mysqlConfigPath)) {
    require_once $mysqlConfigPath;
    if (isset($dsn) && isset($username) && isset($password)) {
        // Parse DSN from mysql_conn.php
        preg_match('/host=([^;]+)/', $dsn, $hostMatch);
        preg_match('/dbname=([^;]+)/', $dsn, $dbMatch);
        $mysqlConfig = [
            'host' => $hostMatch[1] ?? 'unknown',
            'dbname' => $dbMatch[1] ?? 'unknown',
            'username' => $username,
            'password' => $password
        ];
    }
}

if ($mysqlConfig) {
    try {
        $mysqlDsn = "mysql:host={$mysqlConfig['host']};dbname={$mysqlConfig['dbname']};charset=utf8mb4";
        $mysqlConn = new PDO($mysqlDsn, $mysqlConfig['username'], $mysqlConfig['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 5
        ]);
        
        // Test query
        $mysqlStmt = $mysqlConn->query("SELECT VERSION() as version, DATABASE() as database_name, NOW() as server_time");
        $mysqlInfo = $mysqlStmt->fetch(PDO::FETCH_ASSOC);
        
        $results['mysql'] = [
            'name' => 'MySQL Connection',
            'status' => 'PASS',
            'message' => 'MySQL connection successful',
            'details' => [
                'Host' => $mysqlConfig['host'],
                'Database' => $mysqlInfo['database_name'],
                'MySQL Version' => $mysqlInfo['version'],
                'Server Time' => $mysqlInfo['server_time'],
                'Connection Status' => 'Connected'
            ]
        ];
        $mysqlConn = null;
    } catch (PDOException $e) {
        $results['mysql'] = [
            'name' => 'MySQL Connection',
            'status' => 'FAIL',
            'message' => 'MySQL connection failed: ' . $e->getMessage(),
            'details' => [
                'Host' => $mysqlConfig['host'],
                'Database' => $mysqlConfig['dbname'],
                'Error' => $e->getMessage()
            ]
        ];
        $allPassed = false;
    }
} else {
    $results['mysql'] = [
        'name' => 'MySQL Connection',
        'status' => 'FAIL',
        'message' => 'MySQL configuration file not found or invalid',
        'details' => [
            'Config Path' => $mysqlConfigPath,
            'Error' => 'Could not load MySQL configuration'
        ]
    ];
    $allPassed = false;
}

// Test 4: PostgreSQL Connection
// (Config already loaded above)

if ($pgConfig) {
    try {
        $pgDsn = "pgsql:host={$pgConfig['host']};port={$pgConfig['port']};dbname={$pgConfig['dbname']}";
        $pgConn = new PDO($pgDsn, $pgConfig['username'], $pgConfig['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 5
        ]);
        
        // Test query
        $pgStmt = $pgConn->query("SELECT version() as version, current_database() as database_name, now() as server_time");
        $pgInfo = $pgStmt->fetch(PDO::FETCH_ASSOC);
        
        $results['postgresql'] = [
            'name' => 'PostgreSQL Connection',
            'status' => 'PASS',
            'message' => 'PostgreSQL connection successful',
            'details' => [
                'Host' => $pgConfig['host'],
                'Port' => $pgConfig['port'],
                'Database' => $pgInfo['database_name'],
                'PostgreSQL Version' => $pgInfo['version'],
                'Server Time' => $pgInfo['server_time'],
                'Connection Status' => 'Connected'
            ]
        ];
        $pgConn = null;
    } catch (PDOException $e) {
        $results['postgresql'] = [
            'name' => 'PostgreSQL Connection',
            'status' => 'FAIL',
            'message' => 'PostgreSQL connection failed: ' . $e->getMessage(),
            'details' => [
                'Host' => $pgConfig['host'],
                'Port' => $pgConfig['port'],
                'Database' => $pgConfig['dbname'],
                'Error' => $e->getMessage()
            ]
        ];
        $allPassed = false;
    }
} else {
    $results['postgresql'] = [
        'name' => 'PostgreSQL Connection',
        'status' => 'FAIL',
        'message' => 'PostgreSQL configuration file not found or invalid',
        'details' => [
            'Config Path' => $pgConfigPath,
            'Error' => 'Could not load PostgreSQL configuration'
        ]
    ];
    $allPassed = false;
}

// Generate HTML Output
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Server Test Results</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 900px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        .header h1 {
            font-size: 28px;
            margin-bottom: 10px;
        }
        .header .status {
            font-size: 18px;
            opacity: 0.9;
        }
        .status-badge {
            display: inline-block;
            padding: 6px 16px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 14px;
            margin-top: 10px;
        }
        .status-badge.pass {
            background: #10b981;
        }
        .status-badge.fail {
            background: #ef4444;
        }
        .status-badge.warn {
            background: #f59e0b;
        }
        .content {
            padding: 30px;
        }
        .test-section {
            margin-bottom: 30px;
            border: 2px solid #e5e7eb;
            border-radius: 8px;
            overflow: hidden;
        }
        .test-header {
            padding: 20px;
            background: #f9fafb;
            border-bottom: 2px solid #e5e7eb;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .test-header h2 {
            font-size: 20px;
            color: #1f2937;
        }
        .test-body {
            padding: 20px;
        }
        .test-message {
            font-size: 16px;
            color: #4b5563;
            margin-bottom: 15px;
            padding: 12px;
            background: #f3f4f6;
            border-radius: 6px;
            border-left: 4px solid #667eea;
        }
        .details-table {
            width: 100%;
            border-collapse: collapse;
        }
        .details-table td {
            padding: 10px;
            border-bottom: 1px solid #e5e7eb;
        }
        .details-table td:first-child {
            font-weight: 600;
            color: #374151;
            width: 200px;
        }
        .details-table td:last-child {
            color: #6b7280;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            word-break: break-all;
        }
        .footer {
            text-align: center;
            padding: 20px;
            color: #6b7280;
            font-size: 14px;
            border-top: 1px solid #e5e7eb;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Server Test Results</h1>
            <div class="status">
                Overall Status: 
                <span class="status-badge <?php echo strtolower($allPassed ? 'pass' : 'fail'); ?>">
                    <?php echo $allPassed ? '✓ ALL TESTS PASSED' : '✗ SOME TESTS FAILED'; ?>
                </span>
            </div>
        </div>
        
        <div class="content">
            <?php foreach ($results as $key => $result): ?>
                <div class="test-section">
                    <div class="test-header">
                        <h2><?php echo htmlspecialchars($result['name']); ?></h2>
                        <span class="status-badge <?php echo strtolower($result['status']); ?>">
                            <?php echo $result['status']; ?>
                        </span>
                    </div>
                    <div class="test-body">
                        <div class="test-message">
                            <?php echo htmlspecialchars($result['message']); ?>
                        </div>
                        <table class="details-table">
                            <?php foreach ($result['details'] as $label => $value): ?>
                                <tr>
                                    <td><?php echo htmlspecialchars($label); ?></td>
                                    <td><?php echo htmlspecialchars($value); ?></td>
                                </tr>
                            <?php endforeach; ?>
                        </table>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
        
        <div class="footer">
            Test completed at <?php echo date('Y-m-d H:i:s'); ?>
        </div>
    </div>
</body>
</html>
<?php
ob_end_flush();
?>
