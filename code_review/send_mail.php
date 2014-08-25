<?php
/**
 * @title send_mail
 * @description
 * send_mail
 * @author zhangchunsheng423@gmail.com
 * @version V3.0
 * @copyright  Copyright (c) 2010-2014 Luomor Inc. (http://www.luomor.com)
 */
error_reporting(~E_ALL);
/*$to = "zhangchunsheng@yongche.com";
$subject = "review code";
$txt = "review code";
$headers = "From: noreply@yongche.com" . "\r\n";

mail($to, $subject, $txt, $headers);*/

require_once("email.class.php");
$smtpserver = "smtp.263.net";
$smtpserverport = 25;
$smtpusermail = "noreply@yongche.com";
$smtpemailto = "zhangchunsheng@yongche.com,guoxiaodong@yongche.com,wangjing@yongche.com,zhenyun@yongche.com,renxinchang@yongche.com";
$smtpuser = "test@yongche.com";
$smtppass = "";
$mailtitle = "review code";
$mailcontent = "review code";
$mailtype = "TXT";
$smtp = new smtp($smtpserver, $smtpserverport, true, $smtpuser, $smtppass);
$smtp->debug = false;
$smtp->sendmail($smtpemailto, $smtpusermail, $mailtitle, $mailcontent, $mailtype);