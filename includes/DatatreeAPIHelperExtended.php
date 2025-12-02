<?php

/**
 * Extended Datatree API Helper
 * This class contains additional mapping logic for converting numerical codes to descriptive text
 * for property characteristics in the datatree_property table.
 */

class DatatreeAPIHelperExtended {
    
    /**
     * Get Heating Type mapping array
     * @return array
     */
    public static function getHeatingTypeMapping() {
        return [
            1 => 'Baseboard',
            2 => 'Electric',
            3 => 'Central',
            4 => 'Forced air unit',
            5 => 'Oil',
            6 => 'Floor/Wall',
            7 => 'Gravity',
            8 => 'Heat Pump',
            9 => 'Geo-thermal',
            10 => 'Hot Water',
            11 => 'Gas',
            12 => 'Partial',
            13 => 'Radiant',
            14 => 'None',
            15 => 'Other',
            16 => 'Steam',
            17 => 'Coal',
            18 => 'Space/Suspended',
            19 => 'Convection',
            20 => 'Solar',
            21 => 'Vent',
            22 => 'Wood Burning',
            23 => 'Propane',
            24 => 'Yes',
            25 => 'Zone'
        ];
    }

    /**
     * Get Heating Fuel mapping array
     * @return array
     */
    public static function getHeatingFuelMapping() {
        return [
            0 => 'Butane',
            1 => 'Coal',
            2 => 'Electric',
            3 => 'Gas',
            4 => 'Geo-Thermal',
            5 => 'None',
            6 => 'Oil',
            7 => 'Propane',
            8 => 'Solar',
            9 => 'Wood'
        ];
    }

    /**
     * Get Roof Type mapping array
     * @return array
     */
    public static function getRoofTypeMapping() {
        return [
            1 => 'Gable',
            2 => 'Bowstring Truss',
            3 => 'Reinforced Concrete',
            4 => 'Dome',
            5 => 'Steel Frm/Truss',
            6 => 'Flat',
            7 => 'Gable Or Hip',
            8 => 'Hip',
            9 => 'Irr/Cathedral',
            10 => 'Gambrel',
            11 => 'Mansard',
            12 => 'Prestress Concrete',
            13 => 'Rigid Frm Bar Jt',
            14 => 'Shed',
            15 => 'Sawtooth',
            16 => 'Wood Truss',
            17 => 'Monitor',
            18 => 'A-Frame'
        ];
    }

    /**
     * Get Roof Material mapping array
     * @return array
     */
    public static function getRoofMaterialMapping() {
        return [
            1 => 'Asbestos',
            2 => 'Built-Up',
            3 => 'Composition Shingle',
            4 => 'Concrete',
            5 => 'Metal',
            6 => 'Slate',
            7 => 'Rock / Gravel',
            8 => 'Tar & Gravel',
            9 => 'Bermuda',
            10 => 'Masonite / Cement Shake',
            11 => 'Fiberglass',
            12 => 'Aluminum',
            13 => 'Wood Shake / Shingles',
            14 => 'Other',
            15 => 'Asphalt',
            16 => 'Roll Composition',
            17 => 'Steel',
            18 => 'Tile',
            19 => 'Urethane',
            20 => 'Shingle (Not Wood)',
            21 => 'Wood',
            22 => 'Gypsum',
            23 => 'Ceramic Tile',
            24 => 'Clay Tile',
            25 => 'Concrete Tile',
            26 => 'Copper',
            27 => 'Tin',
            28 => 'Solar'
        ];
    }

    /**
     * Get Foundation mapping array
     * @return array
     */
    public static function getFoundationMapping() {
        return [
            1 => 'Adobe',
            2 => 'Brick',
            3 => 'Concrete',
            4 => 'Concrete Block',
            5 => 'Dome',
            6 => 'Frame',
            7 => 'Heavy',
            8 => 'Light',
            9 => 'Log',
            10 => 'Manufactured',
            11 => 'Other',
            12 => 'Masonry',
            13 => 'Metal',
            14 => 'Steel',
            15 => 'Stone',
            16 => 'Tilt-up (pre-cast concrete)',
            17 => 'Wood',
            18 => 'Mixed',
            19 => 'Bamboo'
        ];
    }

    /**
     * Get Exterior Walls mapping array
     * @return array
     */
    public static function getExteriorWallsMapping() {
        return [
            1 => 'Asbestos Shingle',
            2 => 'Brick',
            3 => 'Brick Veneer',
            4 => 'Block',
            5 => 'Composition/Composite',
            6 => 'Concrete',
            7 => 'Concrete Block',
            8 => 'Glass',
            9 => 'Log',
            10 => 'Metal',
            11 => 'Rock, Stone',
            12 => 'Stucco',
            13 => 'Tile',
            14 => 'Tilt-up (pre-cast concrete)',
            15 => 'Other',
            16 => 'Wood Shingle',
            17 => 'Wood',
            18 => 'Wood Siding',
            19 => 'Siding (Alum/Vinyl)',
            20 => 'Adobe',
            21 => 'Shingle (Not Wood)',
            22 => 'Marble',
            23 => 'Combination',
            24 => 'Masonry',
            25 => 'Siding Not (aluminum, vinyl, etc.)',
            26 => 'EIFS / Synthetic Stucco',
            27 => 'Fiber Cement Siding (Hardi-board/Hardi-plank)',
            28 => 'Aluminum Siding',
            29 => 'Vinyl Siding',
            30 => 'Concrete Tile',
            31 => 'Clay Tile',
            32 => 'Ceramic Tile',
            33 => 'Bamboo',
            34 => 'Masonite'
        ];
    }

    /**
     * Get Interior Walls mapping array
     * @return array
     */
    public static function getInteriorWallsMapping() {
        return [
            1 => 'Brick',
            2 => 'Concrete',
            3 => 'Gypsum Board/Drywall/Sheetrock/Wallboard',
            4 => 'Log',
            5 => 'Cement Board',
            6 => 'Plaster',
            7 => 'Stone',
            8 => 'Metal',
            9 => 'Unfinished',
            10 => 'Wood',
            11 => 'Block',
            12 => 'Glass',
            13 => 'Finished/Painted',
            14 => 'Decorative/Custom',
            15 => 'Masonry',
            16 => 'Composition',
            17 => 'Other',
            18 => 'Paneling',
            19 => 'Vinyl',
            20 => 'Plywood/Minimum',
            21 => 'Formica',
            22 => 'Celotex'
        ];
    }

    /**
     * Get Building Quality mapping array
     * @return array
     */
    public static function getBuildingQualityMapping() {
        return [
            1 => 'Buildings having fireproofed structural steel frames carrying all wall, floor and roof loads. Wall, floor and roof structures are built of non-combustible materials.',
            2 => 'Buildings having fireproofed reinforced concrete frames carrying all wall, floor and roof loads which are all non-combustible.',
            3 => 'Exterior walls non-combustible material. Interior partitions and roof structure combustible materials. Floor concrete or wood frame.',
            4 => 'Buildings having wood or wood and steel frames.',
            5 => 'Specialized buildings that do not fit in any of the above categories.'
        ];
    }

    /**
     * Get Building Condition mapping array
     * @return array
     */
    public static function getBuildingConditionMapping() {
        return [
            1 => 'Excellent',
            2 => 'Fair',
            3 => 'Good',
            4 => 'Poor',
            5 => 'Unsound',
            6 => 'Average',
            7 => 'Very Good'
        ];
    }

    /**
     * Get Style mapping array
     * @return array
     */
    public static function getStyleMapping() {
        return [
            1 => 'Traditional / Old',
            2 => 'A-Frame',
            3 => 'Bungalow',
            4 => 'Cape Cod',
            5 => 'Colonial',
            6 => 'English',
            7 => 'French Provincial',
            8 => 'Georgian',
            9 => 'High-Rise',
            10 => 'Modern',
            11 => 'Ranch / Rambler',
            12 => 'Spanish',
            13 => 'Tudor',
            14 => 'Mediterranean',
            15 => 'Conventional',
            16 => 'Other',
            17 => 'Prefab / Modular',
            18 => 'Mansion',
            19 => 'Raised Ranch',
            20 => 'Dome',
            21 => 'Contemporary',
            22 => 'Unfinished / Under Construction',
            23 => 'Victorian',
            24 => 'Cottage',
            25 => 'Custom',
            26 => 'Log Cabin / Rustic',
            27 => 'Historical',
            28 => 'Unknown',
            29 => 'Condo',
            30 => 'Cluster',
            31 => 'Duplex',
            32 => 'Quadplex',
            33 => 'Mobile Home',
            34 => 'Multifamily',
            35 => 'Townhouse',
            36 => 'Triplex',
            37 => 'Patio Home',
            38 => 'Row Home',
            39 => 'Tri-Level',
            40 => 'Bi-Level',
            41 => 'Split Level',
            42 => 'Split Foyer',
            43 => 'Tiny House',
            44 => 'European',
            45 => 'Mobile / Manufactured',
            46 => 'Shotgun',
            47 => 'Bamboo',
            48 => 'Bypass',
            49 => 'Center Hall',
            50 => 'Chalet / Alpine',
            51 => 'Coach / Carriage House',
            52 => 'European',
            53 => 'Federalist',
            54 => 'Garage Apartment',
            55 => 'Greek Revival',
            56 => 'H-Shape',
            57 => 'Low Rise',
            58 => 'L-Shape',
            59 => 'Mission',
            60 => 'Multi-Level',
            61 => 'Mid Rise',
            62 => 'New England',
            63 => 'Old',
            64 => 'Salt Box',
            65 => 'Underground / Berm'
        ];
    }

    /**
     * Get Water Source mapping array
     * @return array
     */
    public static function getWaterSourceMapping() {
        return [
            1 => 'Cistern',
            2 => 'Municipal',
            3 => 'None',
            4 => 'Spring',
            5 => 'Well',
            6 => 'Yes',
            7 => 'Private'
        ];
    }

    /**
     * Get Sewer Type mapping array
     * @return array
     */
    public static function getSewerTypeMapping() {
        return [
            1 => 'Municipal',
            2 => 'None',
            3 => 'Storm',
            4 => 'Septic',
            5 => 'Yes'
        ];
    }

    /**
     * Get Driveway mapping array
     * @return array
     */
    public static function getDrivewayMapping() {
        return [
            1 => 'Driveway',
            2 => 'Driveway Asphalt',
            3 => 'Driveway Bomanite',
            4 => 'Driveway Chat',
            5 => 'Driveway Brick',
            6 => 'Driveway Concrete',
            7 => 'Driveway Paver',
            8 => 'Driveway Gravel',
            9 => 'Driveway Tile'
        ];
    }

    /**
     * Get Topography mapping array
     * @return array
     */
    public static function getTopographyMapping() {
        return [
            'A' => 'Above Street Level',
            'B' => 'Below Street Level',
            'E' => 'Level Grade',
            'H' => 'High Elevation',
            'I' => 'Hilly',
            'L' => 'Low Elevation',
            'M' => 'Mountain',
            'O' => 'Rocky',
            'P' => 'Swampy',
            'Q' => 'Marsh',
            'R' => 'Rolling',
            'S' => 'Steep',
            'W' => 'Wooded',
            'X' => 'Mixed',
            'Y' => 'Brushy'
        ];
    }

    /**
     * Get Amenities mapping array
     * @return array
     */
    public static function getAmenitiesMapping() {
        return [
            1 => 'Arbor/Pergola',
            2 => 'Mobile Home Hookup',
            3 => 'Sauna/Steam Room',
            'A' => 'Carriage House',
            'B' => 'Boat Dock/Ramp',
            'C' => 'Club House',
            'D' => 'Wet Bar',
            'E' => 'Intercom System',
            'F' => 'Safe Room / Panic Room',
            'G' => 'Golf Course/Green',
            'H' => 'Audio Sound System',
            'I' => 'Fire Sprinkler',
            'J' => 'Boat Lift/Davits',
            'K' => 'Outdoor Kitchen/Fireplace',
            'L' => 'Storm or Tornado Shelter/Cellar',
            'M' => 'Smoke/Carbon Monoxide Detector',
            'N' => 'Wine Cellar',
            'O' => 'Basketball/Sport Court',
            'P' => 'Treehouse / Playhouse',
            'Q' => 'Handicap Ramp/Accessible',
            'R' => 'RV Parking',
            'S' => 'Automatic Sprinkler System (lawn / irrigation)',
            'T' => 'Tennis Court',
            'U' => 'Overhead Door',
            'V' => 'Central Vacuum System',
            'W' => 'Water Feature',
            'X' => 'Storm/Security Shutters',
            'Y' => 'Koi Pond'
        ];
    }

    /**
     * Get Cooling Type mapping array
     * @return array
     */
    public static function getCoolingTypeMapping() {
        return [
            1 => 'Central',
            2 => 'Evaporative Cooler',
            3 => 'Office Only',
            4 => 'Packaged Unit',
            5 => 'Window\\Unit',
            6 => 'None',
            7 => 'Other',
            8 => 'Partial',
            9 => 'Chilled Water',
            10 => 'Refrigeration',
            11 => 'Ventilation',
            12 => 'Wall',
            13 => 'Yes',
            14 => 'Geo-Thermal'
        ];
    }

    /**
     * Get Site Influence mapping array
     * @return array
     */
    public static function getSiteInfluenceMapping() {
        return [
            1 => 'Proximity - Freeway',
            2 => 'Waterfront-Beach (Ocean, River or Lake)',
            3 => 'Contamination Site',
            4 => 'Cul-de-sac',
            5 => 'Corner',
            6 => 'View - Negative',
            7 => 'View - Average',
            8 => 'Historical',
            9 => 'Proximity - School',
            10 => 'Golf Course Adjacent',
            11 => 'View - None',
            12 => 'View - Lake',
            13 => 'View - Mountain',
            14 => 'Waterfront - Canal',
            15 => 'View - Ocean',
            16 => 'Proximity - Airport',
            17 => 'Green Belt',
            18 => 'Proximity - Railroad',
            19 => 'Major Street/Thoroughfare',
            20 => 'High Traffic Area',
            21 => 'View - River',
            22 => 'View Not Specified',
            23 => 'Waterfront - Not Specified',
            24 => 'Flood Plain / Flood Zone',
            25 => 'Alley',
            26 => 'View - Bay',
            27 => 'Canal',
            28 => 'City',
            29 => 'Creek',
            30 => 'Industrial',
            31 => 'Inferior',
            32 => 'Poor Access',
            33 => 'Suburban',
            34 => 'Woodland',
            35 => 'Water Front',
            36 => 'Creek Frontage',
            37 => 'Lake Frontage',
            38 => 'Flag Lot',
            39 => 'Small Lake or Pond',
            40 => 'Flood Plain',
            41 => 'Historical',
            42 => 'Wooded Lot',
            43 => 'Transition',
            44 => 'Perimeter',
            45 => 'Interior',
            46 => 'Rural Property',
            47 => 'Busy Street',
            48 => 'Exclusive',
            49 => 'Golf Course',
            50 => 'Highway Frontage',
            51 => 'Bypass',
            52 => 'Type Unknown',
            53 => 'Alley / Corner',
            54 => 'Agricultural',
            55 => 'Airport',
            57 => 'Average',
            58 => 'Bay Access',
            59 => 'Behind Bay Front',
            60 => 'Beaach',
            61 => 'Bay Front',
            62 => 'Behind Gulf Front',
            63 => 'Bay Front Island',
            65 => 'Bay Front Main-Land',
            66 => 'Beach Road',
            67 => 'Business',
            68 => 'Bayou',
            69 => 'Bay',
            71 => 'Canal / Waterfront',
            73 => 'Canal Drainage',
            74 => 'Corner / Extra Front',
            75 => 'Canal Front',
            76 => 'Canal Front Island',
            77 => 'Creek / River',
            78 => 'Corner / Landlock',
            79 => 'Canal Main-Land',
            80 => 'Corner / Woodlot / Waterfront',
            81 => 'Corner / Woodlot',
            82 => 'Corner',
            83 => 'Corner / Restrictions',
            84 => 'Corner / Waterfront',
            85 => 'Cul-de-sac',
            86 => 'Corner / Landlock / Woodlot',
            88 => 'Dead End',
            89 => 'Drainage Easement',
            90 => 'Downtown',
            91 => 'Drainage',
            92 => 'Desirable',
            93 => 'Duplex Lots',
            94 => 'EXTRA FRONT',
            95 => 'Expensive Land',
            96 => 'Extra Front/Restrictions',
            97 => 'Easement',
            98 => 'Flood Line',
            99 => 'Four Plex Lot',
            100 => 'Flood Plain',
            101 => 'Golf Course',
            102 => 'Greenbelt/Golf Course',
            103 => 'Golf/Lake',
            104 => 'Good',
            105 => 'Greenbelt',
            106 => 'Gulf',
            107 => 'Golf/Water',
            108 => 'Highway',
            109 => 'Inside City',
            110 => 'Island Gulf Front',
            111 => 'Industrial',
            112 => 'In-Out City',
            113 => 'Island',
            114 => 'Inland Waterway',
            115 => 'Island No Waterfront',
            116 => 'Lagoon',
            117 => 'Landlocked / Woodlot / Waterfront',
            118 => 'Lake Front',
            119 => 'Lake',
            120 => 'Landlocked',
            121 => 'Lake / Pond',
            122 => 'Landlocked / Woodlot',
            123 => 'Landlocked / Waterfront',
            124 => 'Mobile Home',
            125 => 'Main Land',
            126 => 'Mountain',
            127 => 'Non Buildable',
            128 => 'Intracoastal',
            129 => 'Neighborhood',
            131 => 'Interior',
            132 => 'Ocean',
            133 => 'Open Space',
            134 => 'Outside City',
            135 => 'Park Land',
            136 => 'Poor Access',
            137 => 'Pond',
            138 => 'Perimeter Lot',
            139 => 'Park Front',
            140 => 'Pool',
            141 => 'Preserve',
            143 => 'Private Road',
            144 => 'Ravine',
            145 => 'Recreational',
            146 => 'Rear',
            147 => 'River',
            148 => 'Irregular Lot',
            149 => 'Road',
            150 => 'Restrictions',
            151 => 'Rural',
            152 => 'Sound',
            153 => 'Submerged Land',
            154 => 'Street',
            156 => 'Townhouse',
            157 => 'Tri Plex Lot',
            158 => 'Traffic',
            159 => 'Urban',
            160 => 'Valley',
            161 => 'Water Access',
            162 => 'Water / Cul-De-Sac',
            163 => 'Wetland',
            164 => 'Waterfront',
            166 => 'Waterfalls',
            167 => 'Water',
            168 => 'Woodlot / Waterfront',
            169 => 'Apt / Condo Complex',
            170 => 'Business Cluster',
            171 => 'Central Business',
            172 => 'Commercial / Industrial',
            173 => 'Industrial Size',
            174 => 'Major Strip',
            175 => 'Neighborhood / Spot',
            176 => 'Perm Central Business',
            177 => 'Secondary Bus Strip',
            178 => 'Zero Lot Line',
            179 => 'Type Unknown',
            180 => 'Unit Abuts Elevator',
            181 => 'Average',
            182 => 'Condominimum Hi-Rise',
            183 => 'Condominimum Lo-Rise',
            184 => 'Condo',
            185 => 'Corner Unit',
            186 => 'Condominimum Villas',
            187 => 'Duplex',
            188 => 'End Unit',
            189 => 'Excellent',
            190 => 'Fire Damage',
            191 => 'Front Unit',
            192 => 'Good',
            193 => 'Inferior',
            194 => 'Interior Unit',
            195 => 'Outside City',
            196 => 'Penthouse',
            197 => 'Recreational',
            198 => 'River',
            199 => 'Rear Unit',
            200 => 'Split Plan / Master',
            201 => 'Superior',
            202 => 'Typical',
            203 => 'Exterior Unit',
            204 => 'Apartment / Condo',
            205 => 'Apartment',
            206 => 'Buildable',
            207 => 'City',
            208 => 'Commercial',
            209 => 'Secondary',
            210 => 'Front',
            211 => 'Homesite',
            213 => 'Institutional',
            214 => 'Mobile Home',
            215 => 'Neighborhood',
            216 => 'Primary',
            217 => 'Residual',
            218 => 'Rehabilitation',
            219 => 'Road',
            220 => 'Restaurant',
            221 => 'Rear Unit',
            222 => 'Warehouse',
            223 => 'Excess',
            224 => 'Type Unknown',
            225 => 'Airport',
            226 => 'Average',
            227 => 'Bay',
            228 => 'Best',
            229 => 'Bluff',
            230 => 'Better',
            231 => 'Canal',
            232 => 'City',
            233 => 'Creek / Lake',
            234 => 'Canyon',
            235 => 'Monservation / Protected Area',
            236 => 'Canyon / Valley',
            237 => 'Excellent',
            238 => 'Fair',
            239 => 'Golf Course',
            240 => 'Good',
            241 => 'Greenbelt / Park',
            242 => 'Gulf',
            243 => 'Hills / Mountains',
            244 => 'Hill / Valley',
            245 => 'Inferior',
            246 => 'Interstate',
            247 => 'Inland Waterway',
            248 => 'Intercoastal Waterway',
            249 => 'Lake',
            250 => 'Lagoon',
            251 => 'Lake / Pond',
            252 => 'Mountain',
            253 => 'Mountain / Ocean',
            254 => 'Obstructed',
            255 => 'Ocean',
            256 => 'Park',
            257 => 'Pond',
            258 => 'Parking',
            259 => 'Prime',
            260 => 'Pool',
            261 => 'Premium',
            262 => 'Poor',
            263 => 'Recreational',
            264 => 'Road',
            265 => 'River',
            266 => 'Standard',
            267 => 'Suburban',
            268 => 'Superior',
            269 => 'Street',
            270 => 'Typical',
            271 => 'Woodland',
            272 => 'Water',
            273 => 'Water View'
        ];
    }

    /**
     * Convert Heating Type code to descriptive text
     * @param mixed $heatCode
     * @return string
     */
    public static function convertHeatingTypeCode($heatCode) {
        $mapping = self::getHeatingTypeMapping();
        return isset($mapping[$heatCode]) ? $mapping[$heatCode] : 'Unknown';
    }

    /**
     * Convert Heating Fuel code to descriptive text
     * @param mixed $heatingFuelCode
     * @return string
     */
    public static function convertHeatingFuelCode($heatingFuelCode) {
        $mapping = self::getHeatingFuelMapping();
        return isset($mapping[$heatingFuelCode]) ? $mapping[$heatingFuelCode] : 'Unknown';
    }

    /**
     * Convert Roof Type code to descriptive text
     * @param mixed $roofTypeCode
     * @return string
     */
    public static function convertRoofTypeCode($roofTypeCode) {
        $mapping = self::getRoofTypeMapping();
        return isset($mapping[$roofTypeCode]) ? $mapping[$roofTypeCode] : 'Unknown';
    }

    /**
     * Convert Roof Material code to descriptive text
     * @param mixed $roofMaterialCode
     * @return string
     */
    public static function convertRoofMaterialCode($roofMaterialCode) {
        $mapping = self::getRoofMaterialMapping();
        return isset($mapping[$roofMaterialCode]) ? $mapping[$roofMaterialCode] : 'Unknown';
    }

    /**
     * Convert Foundation code to descriptive text
     * @param mixed $foundationCode
     * @return string
     */
    public static function convertFoundationCode($foundationCode) {
        $mapping = self::getFoundationMapping();
        return isset($mapping[$foundationCode]) ? $mapping[$foundationCode] : 'Unknown';
    }

    /**
     * Convert Exterior Walls code to descriptive text
     * @param mixed $exteriorWallsCode
     * @return string
     */
    public static function convertExteriorWallsCode($exteriorWallsCode) {
        $mapping = self::getExteriorWallsMapping();
        return isset($mapping[$exteriorWallsCode]) ? $mapping[$exteriorWallsCode] : 'Unknown';
    }

    /**
     * Convert Interior Walls code to descriptive text
     * @param mixed $interiorWallsCode
     * @return string
     */
    public static function convertInteriorWallsCode($interiorWallsCode) {
        $mapping = self::getInteriorWallsMapping();
        return isset($mapping[$interiorWallsCode]) ? $mapping[$interiorWallsCode] : 'Unknown';
    }

    /**
     * Convert Building Quality code to descriptive text
     * @param mixed $buildingQualityCode
     * @return string
     */
    public static function convertBuildingQualityCode($buildingQualityCode) {
        $mapping = self::getBuildingQualityMapping();
        return isset($mapping[$buildingQualityCode]) ? $mapping[$buildingQualityCode] : 'Unknown';
    }

    /**
     * Convert Building Condition code to descriptive text
     * @param mixed $buildingConditionCode
     * @return string
     */
    public static function convertBuildingConditionCode($buildingConditionCode) {
        $mapping = self::getBuildingConditionMapping();
        return isset($mapping[$buildingConditionCode]) ? $mapping[$buildingConditionCode] : 'Unknown';
    }

    /**
     * Convert Style code to descriptive text
     * @param mixed $styleCode
     * @return string
     */
    public static function convertStyleCode($styleCode) {
        $mapping = self::getStyleMapping();
        return isset($mapping[$styleCode]) ? $mapping[$styleCode] : 'Unknown';
    }

    /**
     * Convert Water Source code to descriptive text
     * @param mixed $waterCode
     * @return string
     */
    public static function convertWaterSourceCode($waterCode) {
        $mapping = self::getWaterSourceMapping();
        return isset($mapping[$waterCode]) ? $mapping[$waterCode] : 'Unknown';
    }

    /**
     * Convert Sewer Type code to descriptive text
     * @param mixed $sewerCode
     * @return string
     */
    public static function convertSewerTypeCode($sewerCode) {
        $mapping = self::getSewerTypeMapping();
        return isset($mapping[$sewerCode]) ? $mapping[$sewerCode] : 'Unknown';
    }

    /**
     * Convert Driveway code to descriptive text
     * @param mixed $drivewayCode
     * @return string
     */
    public static function convertDrivewayCode($drivewayCode) {
        $mapping = self::getDrivewayMapping();
        return isset($mapping[$drivewayCode]) ? $mapping[$drivewayCode] : 'Unknown';
    }

    /**
     * Convert Topography code to descriptive text
     * @param mixed $topographyCode
     * @return string
     */
    public static function convertTopographyCode($topographyCode) {
        $mapping = self::getTopographyMapping();
        return isset($mapping[$topographyCode]) ? $mapping[$topographyCode] : 'Unknown';
    }

    /**
     * Convert Amenities code to descriptive text
     * @param mixed $amenitiesCode
     * @return string
     */
    public static function convertAmenitiesCode($amenitiesCode) {
        $mapping = self::getAmenitiesMapping();
        return isset($mapping[$amenitiesCode]) ? $mapping[$amenitiesCode] : 'Unknown';
    }

    /**
     * Convert Cooling Type code to descriptive text
     * @param mixed $coolingTypeCode
     * @return string
     */
    public static function convertCoolingTypeCode($coolingTypeCode) {
        $mapping = self::getCoolingTypeMapping();
        return isset($mapping[$coolingTypeCode]) ? $mapping[$coolingTypeCode] : 'Unknown';
    }

    /**
     * Convert Site Influence code to descriptive text
     * @param mixed $siteInfluenceCode
     * @return string
     */
    public static function convertSiteInfluenceCode($siteInfluenceCode) {
        $mapping = self::getSiteInfluenceMapping();
        return isset($mapping[$siteInfluenceCode]) ? $mapping[$siteInfluenceCode] : 'Unknown';
    }

    /**
     * Get Style to Property Type mapping array
     * @return array
     */

    public static function getStyleCodeMapping() {
        return [
            1 => 'Traditional / Old',
            2 => 'A-Frame',
            3 => 'Bungalow',
            4 => 'Cape Cod',
            5 => 'Colonial',
        ];
    }
    public static function getStylePropTypeMapping() {
        return [
            'Traditional / Old' => 'Detached',
            'A-Frame' => 'Detached',
            'Bungalow' => 'Detached',
            'Cape Cod' => 'Detached',
            'Colonial' => 'Detached',
            'English' => 'Detached',
            'French Provincial' => 'Detached',
            'Georgian' => 'Detached',
            'High-Rise' => 'Penthouse Unit/Flat/Apartment',
            'Modern' => 'Detached',
            'Ranch / Rambler' => 'Detached',
            'Spanish' => 'Detached',
            'Tudor' => 'Detached',
            'Mediterranean' => 'Detached',
            'Conventional' => 'Detached',
            'Other' => 'Other',
            'Prefab / Modular' => 'Manufactured',
            'Mansion' => 'Detached',
            'Raised Ranch' => 'Detached',
            'Dome' => 'Detached',
            'Contemporary' => 'Detached',
            'Unfinished / Under Construction' => 'Detached',
            'Victorian' => 'Detached',
            'Cottage' => 'Detached',
            'Custom' => 'Detached',
            'Log Cabin / Rustic' => 'Detached',
            'Historical' => 'Detached',
            'Unknown' => 'Other',
            'Condo' => 'Unit/Flat/Apartment',
            'Cluster' => 'Unit/Flat/Apartment',
            'Duplex' => 'Twin/Semi-Detached',
            'Quadplex' => 'Interior Row/Townhouse',
            'Mobile Home' => 'Mobile Pre 1976',
            'Multifamily' => 'Unit/Flat/Apartment',
            'Townhouse' => 'Interior Row/Townhouse',
            'Triplex' => 'Interior Row/Townhouse',
            'Patio Home' => 'End of Row/Townhouse',
            'Row Home' => 'Interior Row/Townhouse',
            'Tri-Level' => 'Detached',
            'Bi-Level' => 'Detached',
            'Split Level' => 'Detached',
            'Split Foyer' => 'Detached',
            'Tiny House' => 'Detached',
            'European' => 'Detached',
            'Mobile / Manufactured' => 'Manufactured',
            'Shotgun' => 'Detached',
            'Bamboo' => 'Other',
            'Bypass' => 'Other',
            'Center Hall' => 'Unit/Flat/Apartment',
            'Chalet / Alpine' => 'Detached',
            'Coach / Carriage House' => 'Garage/Parking Space',
            'Federalist' => 'Detached',
            'Garage Apartment' => 'Garage/Parking Space',
            'Greek Revival' => 'Detached',
            'H-Shape' => 'Detached',
            'Low Rise' => 'Unit/Flat/Apartment',
            'L-Shape' => 'Detached',
            'Mission' => 'Detached',
            'Multi-Level' => 'Detached',
            'Mid Rise' => 'Unit/Flat/Apartment',
            'New England' => 'Detached',
            'Old' => 'Detached',
            'Salt Box' => 'Detached',
            'Underground / Berm' => 'Other'
        ];
    }

    /**
     * Convert Style name to Property Type
     * @param string $styleName
     * @return string
     */
    public static function convertStyleToPropType($styleName) {
        $mapping = self::getStylePropTypeMapping();
        return isset($mapping[$styleName]) ? $mapping[$styleName] : 'Other';
    }
}
