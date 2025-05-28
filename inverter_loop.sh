 while true
 do
    echo `date +"%Y-%m-%d %H:%M:%S"` $0\($$\) starting gateway inverter msg to database process >>inverters.err
    python inverter_capture.py >>inverters.log  2>>inverters.err
    echo `date +"%Y-%m-%d %H:%M:%S"` $0\($$\) process exited with code $? >>inverters.err
 done
