<?php
/**
 * @title send_mail
 * @description
 * send_mail
 * @author zhangchunsheng423@gmail.com
 * @version V3.0
 * @copyright  Copyright (c) 2010-2014 Luomor Inc. (http://www.luomor.com)
 */
$to = "zhangchunsheng@yongche.com";
$subject = "review code";
$txt = "review code";
$headers = "From: noreply@yongche.com" . "\r\n";

mail($to, $subject, $txt, $headers);