<?php

ini_set("memory_limit", "1024M");
set_time_limit(0);
$whitelisted_ips = array("122.179.196.42", "59.144.172.75", "103.107.57.81");
$max_allowed_ftp = 0;
while (1) {

    //SSH Connections
    $res = exec("who");
    $lines = explode("\n", $res);
    foreach ($lines as $line) {
        $pts_str = substr($line, strpos($line, "pts/"));
        $pts_arr = explode(" ", $pts_str);
        $pts = $pts_arr[0];
        $arr = explode("(", $line);
        $ip_part = $arr[1];
        $ip_arr = explode(")", $ip_part);
        $ip = $ip_arr[0];

        if (!in_array($ip, $whitelisted_ips) && filter_var($ip, FILTER_VALIDATE_IP)) {
            exec("pkill -9 -t " . $pts);
        }
    }

    //FTP Connections
    $ftp = shell_exec("ps -x | grep ftp");
    $lines = explode("\n", $ftp);
    $open_ftp_connections = 0;
    foreach ($lines as $line) {
        if (strpos($line, "/usr/libexec/openssh/sftp-server") !== false) {
            $open_ftp_connections++;
        }
        if ($open_ftp_connections > $max_allowed_ftp) {
            if (strpos($line, "/usr/libexec/openssh/sftp-server") !== false) {
                $pid_arr = explode(" ", $line);
                $pid = $pid_arr[0];
                shell_exec("kill $pid &");
            }
        }
    }
    usleep(100000);
}
