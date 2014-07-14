<?php
    require_once "email.class.php";
    //******************** 配置信息 ********************************
    $smtpserver = "smtp.126.com";//SMTP服务器
    $smtpserverport =25;//SMTP服务器端口
    $smtpusermail = "new2008oh@126.com";//SMTP服务器的用户邮箱
    $smtpemailto = $_POST['toemail'];//发送给谁
    $smtpuser = "new2008oh";//SMTP服务器的用户帐号
    $smtppass = "您的邮箱密码";//SMTP服务器的用户密码
    $mailtitle = $_POST['title'];//邮件主题
    $mailcontent = "<h1>".$_POST['content']."</h1>";//邮件内容
    $mailtype = "HTML";//邮件格式（HTML/TXT）,TXT为文本邮件
    //************************ 配置信息 ****************************
    $smtp = new smtp($smtpserver,$smtpserverport,true,$smtpuser,$smtppass);//这里面的一个true是表示使用身份验证,否则不使用身份验证.
    $smtp->debug = false;//是否显示发送的调试信息
    $state = $smtp->sendmail($smtpemailto, $smtpusermail, $mailtitle, $mailcontent, $mailtype);

    echo "<div style='width:300px; margin:36px auto;'>";
    if($state==""){
        echo "对不起，邮件发送失败！请检查邮箱填写是否有误。";
        echo "<a href='index.html'>点此返回</a>";
        exit();
    }
    echo "恭喜！邮件发送成功！！";
    echo "<a href='index.html'>点此返回</a>";
    echo "</div>";
?>