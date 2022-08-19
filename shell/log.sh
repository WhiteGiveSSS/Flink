#!/bin/bash

nginx_home=/opt/module/nginx
log_home=/opt/gmall0107
log_jar=gmall-logger-0.0.1-SNAPSHOT.jar
case $1 in
"start")
# 先启动nginx

if [[ -z "`ps -ef | awk '/nginx/ && !/awk/ {print $n}'`" ]]; then
# 当字符串为空, 表示nginx没有启动
echo "在hadoop162上启动nginx"
$nginx_home/sbin/nginx
else
echo "nginx已经启动, 无序重复启动..."
fi
#分别在3个节点启动日志服务器
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 上启动日志服务器"
    ssh $host "nohup java -jar $log_home/$log_jar 1>$log_home/run.log 2>&1 &"
done

   ;;
"stop")
echo "在hadoop162上他停止nginx"
$nginx_home/sbin/nginx -s stop
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 上停止日志服务器"
    ssh $host "jps |awk '/$log_jar/ {print \$1}' | xargs kill -9"
done
   ;;

*)
echo "你启动的姿势不对, 换个姿势再来"
echo " log.sh start 启动日志采集"
echo " log.sh stop  停止日志采集"
;;
esac
