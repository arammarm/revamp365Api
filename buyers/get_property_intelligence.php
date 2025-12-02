<?php
ini_set('memory_limit', '4G');
include_once "mysql_conn.php";
include_once "../includes/APIHelper.php";
include_once "../includes/PropertyIntelligenceHelper.php";

APIHelper::Authentication();

global $dsn, $username, $password;
try {
    $conn = new PDO($dsn, $username, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $reqAddress = APIHelper::GetParam('full_street_address', true);
    $reqZip = APIHelper::GetParam('zip', true);
    $reqFips = APIHelper::GetParam('fips', true);

    $zipCode = null;
    if ($reqZip == '') {
        preg_match_all('/\b\d{5}\b/', $reqAddress, $matches);
        if (!empty($matches[0])) {
            $zipCode = end($matches[0]);
        } else {
            echo "No ZIP Code found";
        }
    } else {
        $zipCode = $reqZip;
    }

    if (empty($zipCode) || empty($reqAddress)) {
        APIHelper::SendResponse(null, 0, 'No records found');
        die();
    }

    // Extract house number from the address
    preg_match('/^(\d+)/', trim($reqAddress), $houseNumberMatches);
    $houseNumber = isset($houseNumberMatches[1]) ? $houseNumberMatches[1] : '';
    
    // Validate that we have a house number
    if (empty($houseNumber)) {
        APIHelper::SendResponse(null, 0, 'Invalid address format - house number required');
        die();
    }
    
    // Extract street name (everything after house number)
    $streetName = trim(preg_replace('/^\d+\s*/', '', $reqAddress));
    $normalizedStreetName = normalizeStreetSuffix($streetName);

    // More precise address matching with exact house number validation
    $query = "SELECT 
        PropertyID,
        SitusFullStreetAddress,
        SitusCity, 
        SitusState,
        SitusZIP5,
        FIPS,
        APN,
        TaxAccountNumber,
        Zoning,
        LotSizeAcres_int,
        LotSizeSqFt,
        SumLivingAreaSqFt,
        SumBuildingSqFt,
        SumBasementSqFt,
        Bedrooms,
        BathTotalCalc,
        YearBuilt,
        EffectiveYearBuilt,
        PropertyClassID,
        LandUseCode,
        MobileHomeInd,
        SchoolDistrictName,
        Municipality,
        SubdivisionName,
        Owner1CorpInd,
        Owner2CorpInd,
        Owner1LastName,
        Owner1FirstName,
        Owner1MiddleName,
        Owner1Suffix,
        Owner2LastName,
        Owner2FirstName,
        Owner2MiddleName,
        Owner2Suffix,
        OwnerNAME1FULL,
        OwnerNAME2FULL,
        OwnerOccupied,
        Owner1OwnershipRights,
        MailingFullStreetAddress,
        MailingCity,
        MailingState,
        MailingZIP5,
        MailingOptOut,
        MailingCOName,
        MailingForeignAddressInd,
        AssdTotalValue,
        AssdLandValue,
        AssdImprovementValue,
        MarketTotalValue,
        MarketValueLand,
        MarketValueImprovement,
        TaxAmt,
        TaxYear,
        TaxDeliquentYear,
        MarketYear,
        AssdYear,
        TaxRateCodeArea,
        SchoolTaxDistrict1Code,
        SchoolTaxDistrict2Code,
        SchoolTaxDistrict3Code,
        HomesteadInd,
        VeteranInd,
        DisabledInd,
        WidowInd,
        SeniorInd,
        SchoolCollegeInd,
        ReligiousInd,
        WelfareInd,
        PublicUtilityInd,
        CemeteryInd,
        HospitalInd,
        LibraryInd,
        BuildingArea,
        BuildingAreaInd,
        SumGroundFloorSqFt,
        SumGrossAreaSqFt,
        SumAdjAreaSqFt,
        AtticSqFt,
        AtticUnfinishedSqFt,
        AtticFinishedSqFt,
        BasementUnfinishedSqFt,
        BasementFinishedSqFt,
        SumGarageSqFt,
        GarageUnFinishedSqFt,
        GarageFinishedSqFt,
        TotalRooms,
        BathFull,
        BathsPartialNbr,
        BathFixturesNbr,
        Amenities,
        AirConditioningCode,
        BasementCode,
        BuildingClassCode,
        BuildingConditionCode,
        ConstructionTypeCode,
        DeckInd,
        ExteriorWallsCode,
        InteriorWallsCode,
        FireplaceCode,
        FloorCoverCode,
        Garage,
        HeatCode,
        HeatingFuelTypeCode,
        SiteInfluenceCode,
        GarageParkingNbr,
        DrivewayCode,
        OtherRooms,
        PatioCode,
        PoolCode,
        PorchCode,
        BuildingQualityCode,
        RoofCoverCode,
        RoofTypeCode,
        SewerCode,
        StoriesNbrCode,
        StyleCode,
        SumResidentialUnits,
        SumBuildingsNbr,
        SumCommercialUnits,
        TopographyCode,
        WaterCode,
        LotCode,
        LotNbr,
        LandLot,
        Block,
        Section,
        District,
        LegalUnit,
        Meridian,
        AssessorsMapRef,
        LegalDescription,
        CurrentSaleTransactionId,
        CurrentSaleDocNbr,
        CurrentSaleBook,
        CurrentSalePage,
        CurrentSaleRecordingDate,
        CurrentSaleContractDate,
        CurrentSaleDocumentType,
        CurrentSalesPrice,
        CurrentSalesPriceCode,
        CurrentSaleBuyer1FullName,
        CurrentSaleBuyer2FullName,
        CurrentSaleSeller1FullName,
        CurrentSaleSeller2FullName,
        ConcurrentMtg1DocNbr,
        ConcurrentMtg1Book,
        ConcurrentMtg1Page,
        ConcurrentMtg1RecordingDate,
        ConcurrentMtg1LoanAmt,
        ConcurrentMtg1Lender,
        ConcurrentMtg1Term,
        ConcurrentMtg1InterestRate,
        ConcurrentMtg1LoanDueDate,
        ConcurrentMtg1AdjOrFix,
        ConcurrentMtg1LoanType,
        ConcurrentMtg1TypeFinancing,
        ConcurrentMtg2DocNbr,
        ConcurrentMtg2Book,
        ConcurrentMtg2Page,
        ConcurrentMtg2RecordingDate,
        ConcurrentMtg2LoanAmt,
        ConcurrentMtg2Lender,
        ConcurrentMtg2Term,
        ConcurrentMtg2InterestRate,
        ConcurrentMtg2LoanDueDate,
        ConcurrentMtg2AdjOrFix,
        ConcurrentMtg2LoanType,
        ConcurrentMtg2Typefinancing,
        PrevSaleTransactionId,
        PrevSaleDocNbr,
        PrevSaleBook,
        PrevSalePage,
        PrevSaleRecordingDate,
        PrevSaleContractDate,
        PrevSaleDocumentType,
        PrevSalesPrice,
        PrevSalesPriceCode,
        PrevSaleBuyer1FullName,
        PrevSaleBuyer2FullName,
        PrevSaleSeller1FullName,
        PrevSaleSeller2FullName,
        PrevMtg1DocNbr,
        PrevMtg1Book,
        PrevMtg1Page,
        PrevMtg1RecordingDate,
        PrevMtg1LoanAmt,
        PrevMtg1Lender,
        PrevMtg1Term,
        PrevMtg1InterestRate,
        PrevMtg1LoanDueDate,
        PrevMtg1AdjRider,
        PrevMtg1LoanType,
        PrevMtg1TypeFinancing,
        TotalOpenLienNbr,
        TotalOpenLienAmt,
        Mtg1TransactionId,
        Mtg1RecordingDate,
        Mtg1LoanAmt,
        Mtg1Lender,
        Mtg1PrivateLender,
        Mtg1Term,
        Mtg1LoanDueDate,
        Mtg1AdjRider,
        Mtg1LoanType,
        Mtg1TypeFinancing,
        Mtg1LienPosition,
        Mtg2TransactionId,
        Mtg2RecordingDate,
        Mtg2LoanAmt,
        Mtg2Lender,
        Mtg2PrivateLender,
        Mtg2Term,
        Mtg2LoanDueDate,
        Mtg2AdjRider,
        Mtg2LoanType,
        Mtg2TypeFinancing,
        Mtg2LienPosition,
        Mtg3TransactionId,
        Mtg3RecordingDate,
        Mtg3LoanAmt,
        Mtg3Lender,
        Mtg3PrivateLender,
        Mtg3Term,
        Mtg3LoanDueDate,
        Mtg3AdjRider,
        Mtg3LoanType,
        Mtg3TypeFinancing,
        Mtg3LienPosition,
        Mtg4TransactionId,
        Mtg4RecordingDate,
        Mtg4LoanAmt,
        Mtg4Lender,
        Mtg4PrivateLender,
        Mtg4Term,
        Mtg4LoanDueDate,
        Mtg4AdjRider,
        Mtg4LoanType,
        Mtg4TypeFinancing,
        Mtg4LienPosition,
        CurrentAVMValue,
        vLowValue,
        vHighValue,
        vConfidenceScore,
        vStandardDeviation,
        vValuationDate,
        IsListedFlag,
        IsListedFlagDate,
        IsListedPriceRange,
        flCommunityName,
        flCommunityNbr,
        flFemaMapNbr,
        flFIRMID,
        flPanelNbr,
        flInsideSFHA,
        flFemaFloodZone,
        flFemaMapDate,
        HOA1Name,
        HOA1Type,
        HOA1FeeValue,
        HOA1FeeFrequency,
        HOA2Name,
        HOA2Type,
        HOA2FeeValue,
        HOA2FeeFrequency,
        PFCFlag,
        PFCIndicator,
        PFCReleaseReason,
        PFCTransactionID,
        PFCRecordingDate,
        PFCDocumentType,
        FATimeStamp,
        FARecordType,
        VacantFlag,
        VacantFlagDate
    FROM datatree_property 
    WHERE 
        SitusHouseNbr = :house_number AND
        (lower(SitusStreet) LIKE CONCAT('%', :normalized_street, '%') 
         OR lower(SitusStreet) LIKE CONCAT('%', :original_street, '%')
         OR lower(SitusFullStreetAddress) LIKE CONCAT('%', :normalized_street, '%')
         OR lower(SitusFullStreetAddress) LIKE CONCAT('%', :original_street, '%')) AND
        SitusZIP5 = :zip AND
    (:fips = '' OR FIPS = :fips)
    ORDER BY PropertyID DESC 
    LIMIT 10
    ";

    $stmt = $conn->prepare($query);

    $stmt->bindParam(":house_number", $houseNumber);
    $stmt->bindParam(":normalized_street", $normalizedStreetName);
    $stmt->bindParam(":original_street", $streetName);
    $stmt->bindParam(":zip", $reqZip);
    $stmt->bindParam(":fips", $reqFips);
    $stmt->execute();

    $resultArray = [];
    $currentDate = new DateTime(); // Create current date for intelligence analysis
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        // Analyze property intelligence
        $intelligence = PropertyIntelligenceHelper::analyzeProperty($row, $currentDate);
        
        // Add intelligence data to property
        $propertyData = $row;
        $propertyData['intelligence'] = $intelligence;
        
        $resultArray[] = $propertyData;
    }

    APIHelper::SendResponse($resultArray, 1);
    die();

} catch (PDOException $e) {
    $error = "Query failed: " . $e->getMessage();
    APIHelper::SendResponse([], 0, $error);
}
