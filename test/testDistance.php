<?php
/**
 * @title testDistance
 * @description
 * testDistance
 * @email zhangchunsheng423@gmail.com
 * @date 2014-08-01
 * @version V1.0
 * @copyright  Copyright (c) 2010-2014 Luomor Inc. (http://www.luomor.com)
 */
function indexAutoload($clazz) {
    $file = str_replace('_', '/', $clazz);

    if(is_file("/usr/share/pear/$file.php"))
        require "/usr/share/pear/$file.php";
}

spl_autoload_register('indexAutoload');

$config = new Zend_Config(array(
    'proxy' => array(
        'proxy' => '10.1.5.13:8087',
        'proxyAuth' => ''
    ),
    'oversea_server' => array(
        'map' => 'http://54.254.199.29'
    ),
    'map_baidu' => array(
        'key' => 'DEd51bf09fcded27d8705981745b7351',
    ),
    'map_google' => array(
        'key' => 'AIzaSyBF5oBKe7CNQUddA4bAokOWgSYNq4NmB4I',
    ),
    'map_amap' => array(
        'key' => '17f352fa1e15fd82d377ea0ea939131c',
    ),
));
Zend_Registry::set('config', $config);

$map = new YCL_Map();

/*println($map->getDistance(array(
    array('lng' => -73.994529,'lat' => 40.735243),
    array('lng' => -74.009735,'lat' => 40.705697)
)));*/

println($map->getDistance(array(
    array('lng' => 116.481028,'lat' => 39.989643),
    array('lng' => 114.465302,'lat' => 40.004717)
)));