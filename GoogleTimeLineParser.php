<?php
/**
 * Created by PhpStorm.
 * User: amitguz
 * Date: 1/1/17
 * Time: 10:26 AM
 */

//use taxTrip;
//Drop table location;
//CREATE TABLE `location` (
//`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
//  `jsonData` VARCHAR( 5096 ) NOT NULL ,
//  `lat` FLOAT( 10,7 ) NOT NULL ,
//  `lng` FLOAT( 10, 7 ) NOT NULL,
//  `type` int NOT NULL,
//  `user_id` INT NOT NULL,
//) ENGINE = MYISAM ;
//
//
//ALTER TABLE `location`
//  CHANGE COLUMN `jsonData` `jsonData` VARCHAR(10192);
//
//
// CREATE TABLE `user_jurisdiction` (
//`user_id` INT NOT NULL,
//  `date` DATE not NULL,
//  `jurisdiction` VARCHAR(256)  NOT NULL
//) ENGINE = MYISAM ;
//
//
//ALTER TABLE `user_jurisdiction` ADD UNIQUE `unique_index`(`user_id`, `date`, `jurisdiction`);

set_time_limit(60*60); // can run for one hour
$strJson = file_get_contents('/Users/amitguz/Documents/LocationHistory.json');
$jsonDecode=json_decode($strJson);
$result=array();
$mapDaytoState = array();  //1/1/2016 -> map of states CT, NJ
$mapDaytoCity = array();  //1/1/2016 -> map of cities NYC, Philledelpia
$mapDaytoCounties = array();  //1/1/2016 -> map of countries code IL, US
$mapDaySamples = array();

$i=0;
$numMissing = 0;
$numCityMissing = 0;
$type = 1; //locationIQ = 1, google = 2 , none = -1

$host = '1';
$db   = '';
$user = 'root';
$pass = '';
$charset = 'utf8';

$citiesWithholding = array('NYC' => 'NYC','Philadelphia' =>'Philadelphia');

$dsn = "mysql:host=$host;dbname=$db;charset=$charset";
$opt = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];
$pdo = new PDO($dsn, $user, $pass, $opt);

foreach($jsonDecode as $locations)
{
    foreach($locations as $location) {
        if (isset($location->velocity)) {
            if ($location->velocity>10) {
                //probably driving so skip
                continue;
            }
        }
        $fetchFromDB=true;
        $address = Get_Address_From_db($pdo,$location->latitudeE7/ 10000000, $location->longitudeE7/ 10000000,$type);
        if ($address==null) {
            $fetchFromDB=false;
            if ($type == 2)
                $address = Get_Address_From_Google_Maps($pdo, $location->latitudeE7 / 10000000, $location->longitudeE7 / 10000000, $type);
            elseif ($type == 1)
                $address = Get_Address_From_locationIQ($pdo, $location->latitudeE7 / 10000000, $location->longitudeE7 / 10000000, $type);
            else
                $address = null;
        }
        $mil = $location->timestampMs;
        $seconds = $mil / 1000;
        $day = date("y-m-d", $seconds);

        if (!isset($mapDaySamples[$day])) {
            $mapDaySamples[$day] = 1;//array();
            $mapDaytoState[$day] = array();
            $mapDaytoCity[$day] = array();

        }else{
            $mapDaySamples[$day]++;
        }

        if ($address) {
            //check country
            if (!empty($address['country_code']) && $address['country_code']!='us') {
                if (!isset($mapDaySamples[$day])) {
                    $mapDaytoCounties[$day] = array();
                }
                $mapDaytoCounties[$day][$address['country_code']] = $address['country_code'];
            }else {
                //check state
                if (!empty($address['province'])) {
                    $mapDaytoState[$day][$address['province']] = $address['province'];
                } else {
                    $numMissing++;
                }
                //check city
                if (!empty($address['city'])) {
                    $mapDaytoCity[$day][$address['city']] = $address['city'];
                } else {
                    $numCityMissing++;
                }
            }

        }
        $i++;
        if ($i==20000){
            break;
        }
    }

}

foreach ($mapDaySamples as $day => $numSamples) {
    echo "<br>" . $day . " num samples: " . $numSamples ;
    if (isset($mapDaytoCounties[$day] )) {
        echo "<br>" . $day . " countries :";
        foreach ($mapDaytoCounties[$day] as $county) {
            echo ", " . $county;
            insert_into_user_jurisdiction($pdo, 1, $county, $day,"country");

        }
    }

    if (isset($mapDaytoState[$day] )) {
        echo "<br>" . " states :";
        foreach ($mapDaytoState[$day] as $state) {
            echo ", " . $state;
            insert_into_user_jurisdiction($pdo, 1, $state, $day,"state");

        }
    }
    if (isset($mapDaytoCity[$day] )) {
        echo "<br>" . " cities :";
        foreach ($mapDaytoCity[$day] as $city) {
            echo ", " . $city;
            if (isset($citiesWithholding[$city])) {
                insert_into_user_jurisdiction($pdo, 1, $city, $day,"city");
            }
        }
    }
    echo "<br>";

}

echo "<br>". "Num State missing : " . $numCityMissing . "<br>";
echo "Num City missing : " . $numCityMissing;



function Get_Address_From_db($pdo,$lat, $long,$type)
{
//    $sql = "SELECT jsonData, ( 3959 * acos( cos( radians(:lat) ) * cos( radians( lat ) ) * cos( radians( lng ) - radians(:lng) ) + sin( radians(:lat) ) * sin( radians( lat ) ) ) ) AS distance
//            FROM location l where l.type=:type1 having distance < 1  ORDER BY distance";

    $sql = "SELECT jsonData, ( 3959 * acos( cos( radians($lat) ) * cos( radians( lat ) ) * cos( radians( lng ) - radians($long) ) + sin( radians($lat) ) * sin( radians( lat ) ) ) ) AS distance
            FROM location l where l.type=$type having distance < 0.01  ORDER BY distance";
    $statement=$pdo->query($sql);


//    $data[":lat"]=$lat;
//    $data[":lng"]=$long;
//    $data[":type1"]=$type;
//    $statement = $pdo->prepare($sql);
//    $statement->bindParam(':lat', $lat, PDO::PARAM_STR);
//    $statement->bindParam(':lng', $long, PDO::PARAM_STR);
//    $statement->bindParam(':type1', $type, PDO::PARAM_INT);
////    $statement->execute($data);
//    $result = $statement->setFetchMode(PDO::FETCH_ASSOC);
    $row = $statement->fetch();
    if ($row) {
        // output data of each row
        $jsondata =  json_decode($row["jsonData"],true);

        return Get_Address($jsondata,$type);

    } else {
        return null;
    }
}

function Get_Address($jsondata,$type){
    if ($type==1) {
        return Get_Address_From_locationIQ_json($jsondata);
    }else{
        return Get_Address_From_google_json($jsondata);
    }


}
function Get_Address_From_locationIQ($pdo,$lat, $lon,$type) {
    $url = "http://locationiq.org/v1/reverse.php?format=json&key=dddd&lat=".$lat."&lon=".$lon."&zoom=16";
    $data = @file_get_contents($url);
    // Parse the json response
    $jsondata = json_decode($data,true);

    // If the json data is invalid, return empty array
    if (!check_status_locationIQ($jsondata))   {
        if ($jsondata["error"] == "Rate Limited") {
            //retry
            sleep(1);
            return Get_Address_From_locationIQ($pdo,$lat,$lon,$type);
        }else
            return array();
    }

    insert_into_db_cache($pdo,$lat,$lon, $data,$type);
    return Get_Address_From_locationIQ_json($jsondata);

}

function Get_Address_From_locationIQ_json($jsondata){
    $address = array(
        'country' => locationIQ_getCountry($jsondata),
        'province' => locationIQ_getProvince($jsondata),
        'city' => locationIQ_getCity($jsondata),
        //        'street' => locationIQ_getStreet($jsondata),
        'postal_code' => locationIQ_getPostalCode($jsondata),
        'display_name' => locationIQ_getDisplayname($jsondata),
        'country_code' => locationIQ_getCountryCode($jsondata),
        //        'formatted_address' => locationIQ_getAddress($jsondata),
    );
    return $address;
}

function insert_into_db_cache($pdo,$lat, $long, $json,$type) {

    $address = Get_Address_From_db($pdo,$lat,$long,$type);
    if ($address!=null)
        return;
    try {
        $data = array();

        $data[] = $json;
        $data[] = $lat;
        $data[] = $long;
        $data[] = $type;

        $pdo->prepare("INSERT INTO location (jsonData,lat,lng,type) VALUES (?,?,?,?)")->execute($data);
    } catch (PDOException $e) {
        if ($e->getCode() == 1062) {
            // Take some action if there is a key constraint violation, i.e. duplicate name
        } else {
            throw $e;
        }
    }
}

function insert_into_user_jurisdiction($pdo,$userID, $jurisdiction, $day,$type) {

    try{
        $data = array();

        $data[] = $userID;
        $data[] = $day;//STR_TO_DATE($day,'%m-%d-%y');
        $data[] = $jurisdiction;
        $data[] = $type;

        $pdo->prepare("INSERT INTO user_jurisdiction (user_id,date,jurisdiction,type) VALUES (?,?,?,?)")->execute($data);
    } catch (PDOException $e) {
        if ($e->getCode() == 1062 || $e->getCode()) {
            // Take some action if there is a key constraint violation, i.e. duplicate name
        } else {
            throw $e;
        }
    }
}

function Get_Address_From_Google_json($jsondata) {
    $address = array(
        'country' => google_getCountry($jsondata),
        'province' => google_getProvince($jsondata),
        'city' => google_getCity($jsondata),
        'street' => google_getStreet($jsondata),
        'postal_code' => google_getPostalCode($jsondata),
        'country_code' => google_getCountryCode($jsondata),
        'formatted_address' => google_getAddress($jsondata),
    );

    return $address;
}
function Get_Address_From_Google_Maps($pdo,$lat, $lon,$type) {

    $url = "https://maps.googleapis.com/maps/api/geocode/json?key=ddd&latlng=$lat,$lon&sensor=false";

    // Make the HTTP request
    $data = @file_get_contents($url);
    // Parse the json response
    $jsondata = json_decode($data,true);
    insert_into_db_cache($pdo,$lat,$lon, $data,$type);
    // If the json data is invalid, return empty array
    if (!check_status($jsondata))   return array();

    Get_Address_From_Google_json($jsondata);

}

function locationIQ_getDisplayname($jsondata) {
    if (isset($jsondata["display_name"]))
        return $jsondata["display_name"];
    else
        return "";
}

function locationIQ_getCountry($jsondata) {
    if (isset($jsondata["address"]["country"]))
        return $jsondata["address"]["country"];
    else
        return "";
}

function locationIQ_getCountryCode($jsondata) {
    if (isset($jsondata["address"]["country_code"]))
        return $jsondata["address"]["country_code"];
    else
        return "";
}

function locationIQ_getProvince($jsondata) {
    if (isset($jsondata["address"]["state"]))
        return $jsondata["address"]["state"];
    else
        return "";

}
function locationIQ_getCity($jsondata) {
    if (isset($jsondata["address"]["city"]))
        return $jsondata["address"]["city"];
    else
        return "";
}
//function locationIQ_getStreet($jsondata) {
//    return Find_Long_Name_Given_Type("street_number", $jsondata["results"][0]["address_components"]) . ' ' . Find_Long_Name_Given_Type("route", $jsondata["results"][0]["address_components"]);
//}
function locationIQ_getPostalCode($jsondata) {

    if (isset($jsondata["address"]["postcode"]))
        return $jsondata["address"]["postcode"];
    else
        return "";
}
//function locationIQ_getCountryCode($jsondata) {
//    return Find_Long_Name_Given_Type("country", $jsondata["results"][0]["address_components"], true);
//}
//function locationIQ_getAddress($jsondata) {
//    return $jsondata["results"][0]["formatted_address"];
//}


// ***** GOOGLE ******
function google_getCountry($jsondata) {
    return Find_Long_Name_Given_Type("country", $jsondata["results"][0]["address_components"]);
}
function google_getProvince($jsondata) {
    return Find_Long_Name_Given_Type("administrative_area_level_1", $jsondata["results"][0]["address_components"], true);
}
function google_getCity($jsondata) {
    return Find_Long_Name_Given_Type("locality", $jsondata["results"][0]["address_components"]);
}
function google_getStreet($jsondata) {
    return Find_Long_Name_Given_Type("street_number", $jsondata["results"][0]["address_components"]) . ' ' . Find_Long_Name_Given_Type("route", $jsondata["results"][0]["address_components"]);
}
function google_getPostalCode($jsondata) {
    return Find_Long_Name_Given_Type("postal_code", $jsondata["results"][0]["address_components"]);
}
function google_getCountryCode($jsondata) {
    return Find_Long_Name_Given_Type("country", $jsondata["results"][0]["address_components"], true);
}
function google_getAddress($jsondata) {
    return $jsondata["results"][0]["formatted_address"];
}


/*
* Check if the json data from Google Geo is valid
*/

function check_status($jsondata) {
    if ($jsondata["status"] == "OK") return true;
    return false;
}

/*
* Check if the json data from LocationIQ Geo is valid
*/

function check_status_locationIQ($jsondata) {
    if (isset($jsondata["error"]))
        return false;
    else
        return true;
}
/*
* Searching in Google Geo json, return the long name given the type.
* (If short_name is true, return short name)
*/

function Find_Long_Name_Given_Type($type, $array, $short_name = false) {
    foreach( $array as $value) {
        if (in_array($type, $value["types"])) {
            if ($short_name)
                return $value["short_name"];
            return $value["long_name"];
        }
    }
}