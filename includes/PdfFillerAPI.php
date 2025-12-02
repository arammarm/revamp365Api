<?php

class PdfFillerAPI
{
    const API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjFjMzg3MmU3YjcwZGUwYTg3ODYzMzlmNWYwNjA4MmQ0MTAxYTk5MWUwNTJjZjM2MjYyNmU0ODc0YjZiZjkxMmUzNWIzNjE2NGZiZDA5NWM2In0.eyJhdWQiOiIwIiwianRpIjoiMWMzODcyZTdiNzBkZTBhODc4NjMzOWY1ZjA2MDgyZDQxMDFhOTkxZTA1MmNmMzYyNjI2ZTQ4NzRiNmJmOTEyZTM1YjM2MTY0ZmJkMDk1YzYiLCJpYXQiOjE3NTgzMDg1MjQsIm5iZiI6MTc1ODMwODUyNCwiZXhwIjoxNzg5ODQ0NTI0LCJzdWIiOiI4Mzc5MzEwNDAiLCJzY29wZXMiOltdfQ.VJeDoadmb3AjwoKWxwZjfUqoii5CR9IL0OnWinL9hyeUjaURbBfiqTvdlrIIl8snz5p_Nf-mW-TC-_B-WY_15w87DxyLin7AcUZhKgLKx-fqN1ZhIO0Dsxf5b3rJEJu5QNpuiQ0TxsMAgsbmTErdRauvmMRswBLdqVgSEvTK5s6kF7kBQYYIxasvtLapwyumrxcvoERk0UjZNElQy90JB-oG4nPD7QW5YnuqZVg0QOquybxaFIK4LNjpKROTmIYSNtObJZaPjyWMspw5BxgvaNIwzaMQeFNvkDNl8c0jqy1BlHsCL3FPAg0CkuzRXVKH1YjJB8c2eLuOKn5EAs3NqnD-uvmfw95YdgAka3IYTV41WK73knSOhHGlcjSsCXxtiPdHgBmPz4NEw2O037-u_iWwUcovRWCKGwVWVHNJalJ3dHCl0qDkVe1rDl3Ctjtl4C5ojU9f1dbI194iOYkgfL_CPNogU9o1_j6q5R3jJPzk5IoY7jU3huphj8BKDeQ6Q4n5UHIO-25TOBD2hdTD_YwUL9xgn2vgtlHy0hWzYbRZDmxvIqISckYXbvmkL8rbEG5xkcSyyQumDG9grdpXaNfMWPoHDxcM6lsuGL-2AjE6-k9yRAN4UHfVoxhdZpGLvdiTmKIpWULTDhgIo1HbFi8Wngtu-chogd_7VvHJxtg';

    public array $header = [];

    public function __construct()
    {

    }

    public function getHeader(): array
    {
        $this->header = [
            'Authorization: Bearer ' . self::API_KEY,
            'Content-Type: application/json'
        ];

        return $this->header;
    }


    public function requestAPI($url, $type = 'GET', $postJson = null)
    {
        $curl = curl_init();
        $curlArray = array(
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_ENCODING => '',
            CURLOPT_MAXREDIRS => 10,
            CURLOPT_TIMEOUT => 0,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
            CURLOPT_CUSTOMREQUEST => $type,
            CURLOPT_HTTPHEADER => $this->getHeader(),
        );

        if ($postJson != null) {
            $curlArray[CURLOPT_POSTFIELDS] = $postJson;
        }

        curl_setopt_array($curl, $curlArray);
        $response = curl_exec($curl);
        if (!curl_errno($curl)) {
            $json_data = json_decode($response, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                return $json_data;
            }
        }
        return null;
    }

    public function getTemplateFields($templateId)
    {
        $url = "https://api.pdffiller.com/v2/templates/$templateId/fields?default-error-page=0";
        return $this->requestAPI($url, "GET");
    }

    public function fillTemplate($templateId, $folderId, $templateName, $fillableFieldData)
    {
        $url = "https://api.pdffiller.com/v2/templates/$templateId?default-error-page=0";
        $postBody = [
            'fillable_fields' => $fillableFieldData,
            'name' => $templateName,
            'folder_id' => $folderId
        ];

        return $this->requestAPI($url, "POST", json_encode($postBody));
    }


    public static function getOptionSet(): ?array
    {
        $response = @json_decode(file_get_contents('https://revamp365.ai/version-test/api/1.1/wf/getMlsMappingFields'), true);
        $optionArray = null;
        if (isset($response['status']) && $response['status'] == 'success' && !empty($response['response'])) {
            foreach ($response['response']['field'] as $key => $field) {
                $optionArray[] = [
                    'field' => $field,
                    'display' => $response['response']['display'][$key],
                    'custom' => $response['response']['custom'][$key],
                    'type' => $response['response']['type'][$key]
                ];
            }
        }
        return $optionArray;
    }

    public static function replaceTags($template, $data)
    {
        foreach ($data as $key => $value) {
            $template = str_replace("{{" . $key . "}}", $value, $template);
        }

        return $template;
    }

    public static function getCustomValues($dbResponse, $display)
    {
        $currentDate = date('Y-m-d');

        // Custom Offer Details.
        $offerPrice = APIHelper::GetParam('offer_price', true);
        $offerRatio = APIHelper::GetParam('offer_ratio', true);
        $offerUserName = APIHelper::GetParam('offer_user_name', true);
        $offerUserEmail = APIHelper::GetParam('offer_user_email', true);
        $offerUserAddress = APIHelper::GetParam('offer_user_address', true);
        $offerUserCompanyName = APIHelper::GetParam('offer_user_company_name', true);
        $offerUserCompanyAddress = APIHelper::GetParam('offer_user_company_address', true);
        $offerDescription = APIHelper::GetParam('offer_description', true);
        $offerPriceInText = APIHelper::GetParam('offer_price_in_text', true);

        $resultValue = '';
        if ($display == 'Offer Price') {
            $resultValue = $offerPrice;
        } else if ($display == 'Offer Description') {
            $resultValue = $offerDescription;
        } else if ($display == 'Offer Price In Text') {
            $resultValue = $offerPriceInText;
        } else if ($display == 'Offer Price Ratio Percentage') {
            $resultValue = $offerRatio;
        } else if ($display == 'Offer User Name') {
            $resultValue = $offerUserName;
        } else if ($display == 'Offer User Email') {
            $resultValue = $offerUserEmail;
        } else if ($display == 'Offer User Address') {
            $resultValue = $offerUserAddress;
        } else if ($display == 'Offer User Company Name') {
            $resultValue = $offerUserCompanyName;
        } else if ($display == 'Offer User Company Address') {
            $resultValue = $offerUserCompanyAddress;
        } else if ($display == 'Current Date') {
            $resultValue = $currentDate;
        } else if ($display == 'Date 5 Days Later') {
            $newDate = date('Y-m-d', strtotime($currentDate . ' + 5 days'));
            $resultValue = $newDate;
        } else if ($display == 'Date 7 Days Later') {
            $newDate = date('Y-m-d', strtotime($currentDate . ' + 7 days'));
            $resultValue = $newDate;
        } else if ($display == 'Date 30 Days Later') {
            $newDate = date('Y-m-d', strtotime($currentDate . ' + 30 days'));
            $resultValue = $newDate;
        } else if ($display == 'Complete Address') {
            if (!empty($dbResponse)) {
                @$resultValue = $dbResponse['full_street_address'] . ', ' . $dbResponse['city_name'] . ' ' . $dbResponse['state_or_province'] . ' ' . $dbResponse['zip_code'];
            }
        } else if ($display == 'Property Link') {
            $resultValue = "https://revamp365.ai/?recordid=" . $dbResponse['id'] ?? '';
        }
        return $resultValue;
    }
}

function setValueByType($optionType, $dbValue, $fieldKey, $templateFieldType = null): string
{
    $fields = explode(',', $fieldKey);
    $result = null;
    foreach ($fields as $field) {
        $field = trim($field);
        if (isset($dbValue[$field])) {
            if ($templateFieldType == 'checkmark') {
                $result = ($dbValue[$field]) ? 'ON' : 'OFF';
            }

            if ($optionType == 'number') {
                $result = number_format($dbValue[$field], 0, '.', ',');
            } else if ($optionType == 'price') {
                $result = "$" . number_format($dbValue[$field], 0, '.', ',');
            } else {
                $result .= $dbValue[$field] . ' ';
            }
        }
    }
    return trim($result);
}


