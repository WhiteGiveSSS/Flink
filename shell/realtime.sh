#!/bin/bash
flink="/opt/module/flink-yarn/bin/flink run "
apps_jar=/opt/gmall0107/gmall-realtime-1.0-SNAPSHOT.jar
base_package=com.atguigu.realtime.app

apps=(
#${base_package}.dwd.DwdLogApp
#${base_package}.dwd.DwdDbApp
#${base_package}.dwm.DwmUvApp
#${base_package}.dwm.DwmJumpApp
#${base_package}.dwm.DwmOrderWideApp_Cache_Async
#${base_package}.dwm.DwmPaymentWide
#${base_package}.dws.DwsVisitorStatsApp
#${base_package}.dws.DwsProductStatsApp
#${base_package}.dws.DwsProvinceApp
#${base_package}.dws.DwsKeyWordStatsApp
${base_package}.dws.DwsProductKeyWordStatsApp
)

for app in ${apps[*]} ; do
 ${flink} -d -c ${app} ${apps_jar}
done



