<?php

/**
 * Datatree API Helper
 * This class contains mapping logic for converting numerical codes to descriptive text
 * for property characteristics in the datatree_property table.
 */

class DatatreeAPIHelper {
    const HeaderAPIKey = 'Revamp365-123xyz';
    /**
     * Get Pool mapping array
     * @return array
     */
    public static function getPoolMapping() {
        return [
            0 => 'No Pool',
            1 => 'Above Ground Pool',
            2 => 'Pool & Spa (Both)',
            3 => 'Community Pool Or Spa',
            4 => 'Enclosed',
            5 => 'Heated Pool',
            6 => 'Indoor Swimming Pool',
            7 => 'Solar Heated',
            8 => 'Pool (Yes)',
            9 => 'Spa Or Hot Tub (Only)',
            10 => 'Vinyl In-Ground Pool',
            11 => 'Pool, Historical Value',
            12 => 'In-Ground Pool'
        ];
    }

    /**
     * Convert Pool code to descriptive text
     * @param mixed $poolCode
     * @return string
     */
    public static function convertPoolCode($poolCode) {
        $mapping = self::getPoolMapping();
        return isset($mapping[$poolCode]) ? $mapping[$poolCode] : 'Unknown';
    }

    /**
     * Get Basement mapping array
     * @return array
     */
    public static function getBasementMapping() {
        return [
            1 => 'Daylight, Full',
            2 => 'Full Basement',
            3 => 'Improved Basement (Finished)',
            4 => 'Daylight, Partial',
            5 => 'No Basement',
            6 => 'Partial Basement',
            7 => 'Unfinished Basement',
            8 => 'Unspecified Basement',
            9 => 'Unspecified Basement',
            10 => 'Daylight / Walkout'
        ];
    }

    /**
     * Get Stories mapping array
     * @return array
     */
    public static function getStoriesMapping() {
        return [
            100 => '1 Story',
            125 => '1.25 Stories',
            150 => '1.5 Stories',
            175 => '1.75 Stories',
            200 => '2 Stories',
            225 => '2.25 Stories',
            250 => '2.5 Stories',
            275 => '2.75 Stories',
            300 => '3 Stories',
            325 => '3.25 Stories',
            350 => '3.5 Stories',
            375 => '3.75 Stories',
            400 => '4 Stories',
            425 => '4.25 Stories',
            450 => '4.5 Stories',
            475 => '4.75 Stories',
            500 => '5 Stories',
            525 => '5.25 Stories',
            550 => '5.5 Stories',
            575 => '5.75 Stories',
            600 => '6 Stories',
            650 => '6.5 Stories',
            700 => '7 Stories',
            750 => '7.5 Stories',
            800 => '8 Stories',
            850 => '8.5 Stories',
            900 => '9 Stories',
            950 => '9.5 Stories',
            1000 => '10 Stories',
            1100 => '11 Stories',
            1200 => '12 Stories',
            1300 => '13 Stories',
            1400 => '14 Stories',
            1500 => '15 Stories',
            1600 => '16 Stories',
            1700 => '17 Stories',
            1800 => '18 Stories',
            1900 => '19 Stories',
            2000 => '20 Stories',
            2100 => '21 Stories',
            2200 => '22 Stories',
            2300 => '23 Stories',
            2400 => '24 Stories',
            2500 => '25 Stories',
            2600 => '26 Stories',
            2700 => '27 Stories',
            2800 => '28 Stories',
            2900 => '29 Stories',
            3000 => '30 Stories',
            3100 => '31 Stories',
            3200 => '32 Stories',
            3300 => '33 Stories',
            3400 => '34 Stories',
            3500 => '35 Stories',
            3600 => '36 Stories',
            3700 => '37 Stories',
            3800 => '38 Stories',
            3900 => '39 Stories',
            4000 => '40 Stories',
            4100 => '41 Stories',
            4200 => '42 Stories',
            4300 => '43 Stories',
            4400 => '44 Stories',
            4500 => '45 Stories',
            4600 => '46 Stories',
            4700 => '47 Stories',
            4800 => '48 Stories',
            4900 => '49 Stories',
            5000 => '50 Stories',
            5100 => '51 Stories',
            5200 => '52 Stories',
            5300 => '53 Stories',
            5400 => '54 Stories',
            5500 => '55 Stories',
            5600 => '56 Stories',
            5700 => '57 Stories',
            5800 => '58 Stories',
            5900 => '59 Stories',
            6000 => '60 Stories',
            6100 => '61 Stories',
            6200 => '62 Stories',
            6300 => '63 Stories',
            6400 => '64 Stories',
            6500 => '65 Stories',
            6600 => '66 Stories',
            6700 => '67 Stories',
            6800 => '68 Stories',
            6900 => '69 Stories',
            7000 => '70 Stories',
            7100 => '71 Stories',
            7200 => '72 Stories',
            7300 => '73 Stories',
            7400 => '74 Stories',
            7500 => '75 Stories',
            7600 => '76 Stories',
            7700 => '77 Stories',
            7800 => '78 Stories',
            7900 => '79 Stories',
            8000 => '80 Stories',
            8100 => '81 Stories',
            8200 => '82 Stories',
            8300 => '83 Stories',
            8400 => '84 Stories',
            8500 => '85 Stories',
            8700 => '87 Stories',
            8800 => '88 Stories',
            8900 => '89 Stories',
            9000 => '90 Stories',
            9100 => '91 Stories',
            9200 => '92 Stories',
            9300 => '93 Stories',
            9400 => '94 Stories',
            9500 => '95 Stories',
            9600 => '96 Stories',
            9700 => '97 Stories',
            9800 => '98 Stories',
            9900 => '99 Stories',
            10000 => '100 Stories',
            10400 => '104 Stories',
            10500 => '105 Stories',
            11200 => '112 Stories',
            11400 => '114 Stories',
            11900 => '119 Stories',
            12500 => '125 Stories'
        ];
    }

    /**
     * Convert Basement code to descriptive text
     * @param mixed $basementCode
     * @return string
     */
    public static function convertBasementCode($basementCode) {
        $mapping = self::getBasementMapping();
        return isset($mapping[$basementCode]) ? $mapping[$basementCode] : 'Unknown';
    }

    /**
     * Convert Stories code to descriptive text
     * @param mixed $storiesCode
     * @return string
     */
    public static function convertStoriesCode($storiesCode) {
        $mapping = self::getStoriesMapping();
        return isset($mapping[$storiesCode]) ? $mapping[$storiesCode] : 'Unknown';
    }

    /**
     * Transform property data by converting all codes to descriptive text
     * @param array $row
     * @return array
     */
    public static function transformPropertyData($row) {
        $transformedRow = $row;
        
        // Convert Pool code to text
        if (isset($row['PoolCode'])) {
            $transformedRow['PoolCode'] = self::convertPoolCode($row['PoolCode']);
        }
        
        // Convert Basement code to text
        if (isset($row['BasementCode'])) {
            $transformedRow['BasementCode'] = self::convertBasementCode($row['BasementCode']);
        }
        
        // Convert Stories code to text
        if (isset($row['StoriesNbrCode'])) {
            $transformedRow['StoriesNbrCode'] = self::convertStoriesCode($row['StoriesNbrCode']);
        }
        
        // Convert Heating Type code to text
        if (isset($row['HeatCode'])) {
            $transformedRow['HeatCode'] = DatatreeAPIHelperExtended::convertHeatingTypeCode($row['HeatCode']);
        }
        
        // Convert Heating Fuel code to text
        if (isset($row['HeatingFuelTypeCode'])) {
            $transformedRow['HeatingFuelTypeCode'] = DatatreeAPIHelperExtended::convertHeatingFuelCode($row['HeatingFuelTypeCode']);
        }
        
        // Convert Roof Type code to text
        if (isset($row['RoofTypeCode'])) {
            $transformedRow['RoofTypeCode'] = DatatreeAPIHelperExtended::convertRoofTypeCode($row['RoofTypeCode']);
        }
        
        // Convert Roof Material code to text
        if (isset($row['RoofCoverCode'])) {
            $transformedRow['RoofCoverCode'] = DatatreeAPIHelperExtended::convertRoofMaterialCode($row['RoofCoverCode']);
        }
        
        // Convert Foundation code to text
        if (isset($row['ConstructionTypeCode'])) {
            $transformedRow['ConstructionTypeCode'] = DatatreeAPIHelperExtended::convertFoundationCode($row['ConstructionTypeCode']);
        }
        
        // Convert Exterior Walls code to text
        if (isset($row['ExteriorWallsCode'])) {
            $transformedRow['ExteriorWallsCode'] = DatatreeAPIHelperExtended::convertExteriorWallsCode($row['ExteriorWallsCode']);
        }
        
        // Convert Interior Walls code to text
        if (isset($row['InteriorWallsCode'])) {
            $transformedRow['InteriorWallsCode'] = DatatreeAPIHelperExtended::convertInteriorWallsCode($row['InteriorWallsCode']);
        }
        
        // Convert Building Quality code to text
        if (isset($row['BuildingQualityCode'])) {
            $transformedRow['BuildingQualityCode'] = DatatreeAPIHelperExtended::convertBuildingQualityCode($row['BuildingQualityCode']);
        }
        
        // Convert Building Condition code to text
        if (isset($row['BuildingConditionCode'])) {
            $transformedRow['BuildingConditionCode'] = DatatreeAPIHelperExtended::convertBuildingConditionCode($row['BuildingConditionCode']);
        }
        
        // Convert Style code to text
        if (isset($row['StyleCode'])) {
            $transformedRow['StyleCode'] = DatatreeAPIHelperExtended::convertStyleCode($row['StyleCode']);
        }
        
        // Convert Water Source code to text
        if (isset($row['WaterCode'])) {
            $transformedRow['WaterCode'] = DatatreeAPIHelperExtended::convertWaterSourceCode($row['WaterCode']);
        }
        
        // Convert Sewer Type code to text
        if (isset($row['SewerCode'])) {
            $transformedRow['SewerCode'] = DatatreeAPIHelperExtended::convertSewerTypeCode($row['SewerCode']);
        }
        
        // Convert Driveway code to text
        if (isset($row['DrivewayCode'])) {
            $transformedRow['DrivewayCode'] = DatatreeAPIHelperExtended::convertDrivewayCode($row['DrivewayCode']);
        }
        
        // Convert Topography code to text
        if (isset($row['TopographyCode'])) {
            $transformedRow['TopographyCode'] = DatatreeAPIHelperExtended::convertTopographyCode($row['TopographyCode']);
        }
        
        // Convert Amenities code to text
        if (isset($row['Amenities'])) {
            $transformedRow['Amenities'] = DatatreeAPIHelperExtended::convertAmenitiesCode($row['Amenities']);
        }
        
        // Convert Cooling Type code to text
        if (isset($row['AirConditioningCode'])) {
            $transformedRow['AirConditioningCode'] = DatatreeAPIHelperExtended::convertCoolingTypeCode($row['AirConditioningCode']);
        }
        
        // Convert Site Influence code to text
        if (isset($row['SiteInfluenceCode'])) {
            $transformedRow['SiteInfluenceCode'] = DatatreeAPIHelperExtended::convertSiteInfluenceCode($row['SiteInfluenceCode']);
        }
        
        // Add StylePropType field based on StyleCode
        if (isset($row['StyleCode'])) {
            $styleName = DatatreeAPIHelperExtended::convertStyleCode($row['StyleCode']);
            $transformedRow['StylePropType'] = DatatreeAPIHelperExtended::convertStyleToPropType($styleName);
        }
        
        return $transformedRow;
    }
}
