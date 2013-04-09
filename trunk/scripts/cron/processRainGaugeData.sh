#!/bin/sh
cd /home/xeniaprod/scripts/dhec; 
#Process the radar rain data.
/usr/bin/python getRainData.py --XMLConfigFile=/home/xeniaprod/config/dhecConfig.xml --GetXMRGData > /home/xeniaprod/tmp/log/processRainGaugeData.log 2>&1
#Run the prediction tests
/usr/bin/python beachAdvisoryTests.py --XMLConfigFile=/home/xeniaprod/config/dhecConfig.xml >> /home/xeniaprod/tmp/log/processRainGaugeData.log 2>&1
#perl obskml_to_xenia_sqlite.pl http://neptune.baruch.sc.edu/xenia/feeds/xenia_obskml_latest.kmz /home/xeniaprod/tmp/dhec latest_dhec ./dhec.db /home/xeniaprod/tmp/dhec >> /home/xeniaprod/tmp/log/processRainGaugeData.log
#bash cleanup.sh

/usr/bin/python getRainData.py --XMLConfigFile=/home/xeniaprod/config/dhecConfig.xml --Vacuum >> /home/xeniaprod/tmp/log/processRainGaugeData.log 2>&1