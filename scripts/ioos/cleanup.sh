find /home/dramage/scripts/dhec/ioos/archive_in -cmin +1400 -exec rm -f {} \;

#cleanup old tmp files - careful rm -rf only on tmp
rm -rf /home/dramage/scripts/dhec/ioos/tmp/gearth_*
rm -f /home/dramage/scripts/dhec/ioos/tmp/*.log

