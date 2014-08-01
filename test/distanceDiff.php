<?php
/**
 * @title distanceDiff
 * @description
 * distanceDiff
 * @email zhangchunsheng423@gmail.com
 * @date 2014-08-01
 * @version V1.0
 * @copyright  Copyright (c) 2010-2014 Luomor Inc. (http://www.luomor.com)
 */
$content = file_get_contents("baidu.txt");
$content = explode("\n", $content);

$file = fopen("distanceDiff.txt", "w+");

/**
 * {
 *  distance: 1927861,
 *  duration: 77114,
 *  driver: 77114,
 *  taxi_amount: 0,
 *  type: "amap"
 * }
 */
$diff = 0.1;

$count_distance_diff = 0;
$count_duration_diff = 0;
$count = 0;

foreach($content as $value) {
    $url = $value . "&method=baidu";
    $result_baidu = request($url);
    fwrite($file, $result_baidu . "\n");

    $url = $value . "&method=amap";
    $result_amap = request($url);
    fwrite($file, $result_amap . "\n");

    $result_baidu = json_decode($result_baidu);
    $result_amap = json_decode($result_amap);

    if(isset($result_baidu->distance)) {
        $distance_baidu = $result_baidu->distance;
        $distance_amap = $result_amap->distance;

        $duration_baidu = $result_baidu->duration;
        $duration_amap = $result_amap->duration;

        $stand_distance_diff = $distance_baidu * $diff;
        $stand_duration_diff = $duration_baidu * $diff;

        $distance_diff = abs($distance_baidu - $distance_amap);
        $duration_diff = abs($duration_baidu - $duration_amap);

        $count++;

        if($distance_diff > $stand_distance_diff) {
            $count_distance_diff++;
        }

        if($duration_diff > $stand_duration_diff) {
            $count_duration_diff++;
        }
    }
}

fwrite($file, "距离误差大于10%的比例为" . ($count_distance_diff / $count) . "\n");
fwrite($file, "时间误差大于10%的比例为" . ($count_duration_diff / $count) . "\n");

fclose($file);

function request($url, $method = "GET", $post_fields = null, $header = array()) {
    $ch = curl_init();

    try {
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5);
        curl_setopt($ch, CURLOPT_TIMEOUT, 10);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $header);

        if($method == "POST") {
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $post_fields);
        }
        $result = curl_exec($ch);

        if(curl_error($ch)) {
            error_log("access $url error:" . curl_error($ch));
        }
        curl_close($ch);
    } catch(Exception $e) {
        curl_close($ch);
        throw $e;
    }

    if(empty($result)) {
        return false;
    }

    return $result;
}