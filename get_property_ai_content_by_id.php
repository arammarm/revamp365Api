<?php
require_once '../config.php';
require_once 'includes/APIHelper.php';
global $db;

APIHelper::Authentication();
$queryParams = [];
$whereClause = ' WHERE 1 = 1 ';
if (APIHelper::GetParam('id') != null) {
    $queryParams [] = APIHelper::GetParam('id');
    $whereClause .= " AND id = $" . count($queryParams);

    $query = pg_query($db, "
SELECT data_type, column_name
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'properties';
");
    $result = pg_fetch_all($query);
    $columnKeys = [];
    $columnKeys = ['real_dom' => ''];
    foreach ($result as $queryOption) {
        $columnKeys[$queryOption['column_name']] = $queryOption['data_type'];
    }
    $query = pg_query_params($db, "SELECT *, EXTRACT(DAY FROM (NOW() - listing_entry_date)) as real_dom FROM properties $whereClause LIMIT 1", $queryParams);
    if ($row = pg_fetch_assoc($query)) {

        $dataString = <<<EOT
Information Type: Property Details,
Revamp ID: ${row['id']},
TAX ID Number: ${row['tax_id_number']},
DOM(Days on Market): ${row['dom']},
Street Address(Full Street Address): ${row['full_street_address']},
City: ${row['city_name']},
State Or Province: ${row['state_or_province']},
Zip Code: ${row['zip_code']},
MLS Number: ${row['mls_number']},
Zoning: ${row['zoning']},
Status: ${row['status']},
County: ${row['county']},
Bedroom Count: ${row['bedrooms_count']},
Bathroom Count: ${row['bathrooms_total_count']},
Structure Type(Property Type): ${row['structure_type']},
Closed Date: ${row['close_date']},
Buyer Financing: ${row['buyer_financing']},
Listing Office Name: ${row['list_office_name']},
Listing Agent First Name: ${row['list_agent_first_name']},
Listing Agent Last Name: ${row['list_agent_last_name']},
Listing Agent Email: ${row['list_agent_email']},
Listing Agent Cell: ${row['list_agent_cell']},
List Price: ${row['list_price']},
Close Price: ${row['close_price']},
Total Finished Sqft(Total Sqft): ${row['total_finished_sqft']},
Price Per Sqft: ${row['price_per_sqft']},
Lot Sqft: ${row['lot_sqft']},
Improvement Assessed Value: ${row['improvement_assessed_value']},
Land Assessed Value: ${row['land_assessed_value']},
Tax Assessed Value: ${row['tax_assessed_value']},
Tax Annual Amount: ${row['tax_annual_amount']},
Heating Type: ${row['heating_type']},
Cooling Type: ${row['cooling_type']},
Remarks Public: ${row['remarks_public']},
Low Rent Price: ${row['lowrent']},
Comparable Rentals Count: ${row['comparablerentals']},
Median Rent Price: ${row['medianrent']},
Percentile 25: ${row['percentile25']},
Percentile 75: ${row['percentile75']},
High Rent Price: ${row['highrent']},
Average List: ${row['avg_list']},
Average Closed Price: ${row['avg_c_price']},
Average Price Per Sqft: ${row['avg_p_sqft']},
Average DOM(Days on Market): ${row['avg_dom']},
Average Sqft: ${row['avg_sqft']},
Sales: ${row['sales']},
High Comparable Cluster: ${row['high_comps_cluster']},
High Comparable Price Per Sqft(High PPSF): ${row['high_comps_ppsf']},
Low Comparable Cluster: ${row['low_comps_cluster']},
Low Comparable Price Per Sqft(Low PPSF): ${row['low_comps_ppsf']},
Estimate Automated Valuation Models(Est AVM): ${row['est_avm']},
Estimate After Repair Value(Est ARV): ${row['est_arv']},
Delta Price Per Sqft(Delta PPSF): ${row['delta_psf']},
Estimate Full Rehab: ${row['est_full_rehab']},
Estimate Profit: ${row['est_profit']},
Estimate Cashflow: ${row['est_cashflow']},
Record Modification Date: ${row['modification_timestamp']},
Listing Entry Date: ${row['listing_entry_date']},
Building Units Total: ${row['building_units_total']},
Year Built: ${row['year_built']},
Price Per Sqft After Closed: ${row['price_per_sqft_closed']},
Geo Address: ${row['geo_address']},
Accuracy Score Value Out of 10: ${row['accuracy_score_value']},
Accuracy Score Rent Out of 10: ${row['accuracy_score_rent']},
Latitude: ${row['latitude']},
Longitude: ${row['longitude']},
HOA: ${row['hoa_yn']},
Deal Type: ${row['wholesale']},
DOM(Days on Market) From Listing Entry Date: ${row['real_dom']}
EOT;
        $dataString = str_replace('"', '“', $dataString,);
        $dataString = 'Generate responses based on "' . $dataString . '"';

//        die($dataString);
         APIHelper::SendResponse($dataString, 1);
        exit();
    }
}
 APIHelper::SendResponse([], 0, 'The request or response is invalid.');