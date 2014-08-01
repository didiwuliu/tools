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

foreach($content as $value) {
    $url = $value;
}