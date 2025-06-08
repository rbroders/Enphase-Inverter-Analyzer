# Enphase-Inverter-Analyzer
This utility stores individual inverter output in a database and analyzes it to look for power shaving losses.

It is made up of two parts: inverter_capture.py runs continuously (especially during the light), capturing
local inverter production data and storing it in a database, and inverter_analyzer.py which can be run to analyze
the database data to help determine if the inverters are undersized.  This package supports MySQL/MariaDB as 
well as sqlite.

This program would not be possible without Matthew1471's [Enphase-API](https://github.com/Matthew1471/Enphase-API).
Thanks a lot Matthew - stellar work!

# Installation
Install python.  Using the windows store is easiest.
Verify version with `python --version`
I am not certion the minimum python version required.  It was developed with 3.13.1.  I capture from my QNAP NAS on 3.12.6.

Install specialized packages:
```
python -m pip install requests
python -m pip install enphase_api
python -m pip install numpy
python -m pip install matplotlib
```
If you intend to use a MySQL/MariaDB you will also need to install mysql-connector-python.

Download `inverter_capture.py credentials.json and inverter_analyzer.py` from this git to a directory somewhere.
To connect to your Enphase Gateway, you will need to edit credentials.json.  Specifically you need a token:
[enphase local api](https://enphase.com/download/accessing-iq-gateway-local-apis-or-local-ui-token-based-authentication)

NOTE: the [entrez web-site](https://entrez.enphaseenergy.com/) can be finicky.  After logging in it will ask you to "Select System".  
This is your system name which can be found above Site ID in the upper left of the ENPHASE app's MENU tab.  
My system name is my first and last name.  Its important to let the entrez web-page auto-complete your system name, so type slowly,
and click on the drop down box when it appears with your site name and site id.
This will populate the "Select Gateway" drop down, so use the dropdown (**v**) and select your gateway serial number from it.  
Now you can press "Create access token", and your access token will appear on the next page.  Copy it into 
credentials.json.

You can also use the token to access your gateway's built-in web-page: https://envoy.local.  You will have to skip past your
browser's security warnings and the gateway's "Sorry you are not authorized to view that page" message, but then
you can paste in the token and access the meager web-site. It is useful for setting up a Static IP address.
I think the token only lasts for a year, so we may have to repeat this process periodically.

You can also enter MySQ/MariaDB database parameters in your credentials file instead of using command line.

# inverter_capture.py
The inverter_capture utility connects to your local enphase gateway and uses the 
[/api/v1/production/inverters](https://github.com/Matthew1471/Enphase-API/blob/main/Documentation/IQ%20Gateway%20API/V1/Production/Inverters.adoc)
endpoint to capture inverter data.  Helpfully, the inverters timestamp their readings so I don't have to store
repeat readings.  When the gateway is queried, it sends the most recent inverter data it has received.  
Inverter_capture tracks the messages and ignores resends.  A message containing nothing but resends is called stale.
At night the inverters stop updating their output, so all messages become stale.  I also ignore duplicate readings 
in an attempt to reduce database space (the analyze program regenerates them to recreate smooth data).

The inverter_capture.py utility must run continuously while the inverters are producing power to track their output.
I run mine on a QNAP NAS with a MariaDB, but the setup was complicated and annoying.  It is also possible to 
run using SQLite which has no security and setup is completely automatic (including schema).  Also, the SQLite database operates
in WAL mode, so all access must be from the same computer.  This is needed because the analysis tool has a long
running QUERY and in rollback journal mode this causes writer starvation, blocking the capture utility.

Command line arguments control database connection and poll frequency (use -help).  My inverters send new data
every 331 seconds, so the default poll interval is 60 seconds.  Progress is logged to stdout every 5 minutes
and errors are sent to stderr.  The program is designed to run continuously so it captures all signals and
attempts to restart itself if something goes wrong with the gateway or the database.  The program may take
up to 1 minute to exit cleanly after pressing Ctrl-C.

If you are using windows make sure to modify your Power & Sleep settings so your system **Never** sleeps.
The program is designed to recover gracefully, so restarting within a few (5) minutes will not result in data loss.

```console
python inverter_capture.py -DBFile inverters.db
2025-05-25 21:44:07.459668 Capturing 7 signals
2025-05-25 21:44:07.460496 Starting up as pid 468 version 0.9.3
2025-05-25 21:44:07.460975 Loaded credentials.json
2025-05-25 21:44:07.847687 Connected to Enphase gateway at https://192.168.0.31
2025-05-25 21:44:07.849900 Connected to SQLite database inverters.db (version=3.45.3, journal_mode=wal).
2025-05-25 21:44:08.003894 Found 44 inverters in the database producing 0 watts
2025-05-25 21:44:08.004114 Collecting inverter readings. To exit press CTRL+C
2025-05-25 21:49:08 6 msgs (5 stale) stored 0 readings (ignored 223 resends, 41 unchanged) 0 watts
2025-05-25 21:54:08 5 msgs (5 stale) stored 0 readings (ignored 220 resends, 0 unchanged) 0 watts
...
2025-05-27 13:19:03 5 msgs (0 stale) stored 33 readings (ignored 183 resends, 4 unchanged) 15312 watts
2025-05-27 13:24:03 5 msgs (0 stale) stored 27 readings (ignored 182 resends, 11 unchanged) 15307 watts
2025-05-27 13:29:04 5 msgs (0 stale) stored 30 readings (ignored 180 resends, 10 unchanged) 15291 watts
...
2025-05-27 20:34:03 5 msgs (1 stale) stored 0 readings (ignored 200 resends, 20 unchanged) 0 watts
2025-05-27 20:39:03 5 msgs (5 stale) stored 0 readings (ignored 220 resends, 0 unchanged) 0 watts
2025-05-27 20:43:30.355864 Received signal SIGINT interrupt (2) in C:\Users\Bob\source\repos\Enphase-Inverter-Analyzer\inverter_capture.py@main(479)
2025-05-27 20:44:03.963960 Goodbye.
```



# inverter_analyzer.py
Once the capture utility has been running for a complete day (sun-up to sun-down), you can run inverter_analyzer.py
This program analyzes the inverter output and produces a report with a line for each inverter every day (-Detail True).
It does this by attempting to map a parabola to the non-exceedance inverter output (i.e. output less than max continuous).
To be successful, data for an inverter-day is eliminated if it does not meet the following criteria: 
MAX_START_POWER < 20 and MIN_END_POWER = 0 and at least 50 data points (daily data must be complete).
All data below 75W is ignored as the trail-in and trail-out data is not very parabolic.
All data above the MAX_CONTINUOUS parameter is also ignored as the true output could be higher.
Then the system fits a "cloud limit" parabola to the data and looks for power drop-outs (5W or more below expected output).
These "cloudy" points are ignored and a "cloud limit 2" parabola is fitted to the remaining data.  
The second set of cloudy points is ignored and a final parabola is fitted to the data.
Amazingly, many of the original "cloudy" points are restored as the new parabolas do a better job of fitting the true data.
The number in parenthesis in the legend shows how many "cloudy" points are under each parabola.

Once this is complete, the EXCEEDANCE energy is calculated.  EXCEEDANCE energy is the power generated beyond
MAX_CONTINUOUS (integrated over time).  My system has IQ8A inverters with a MAX_CONTINUOUS rating of 349W, 
but their PEAK rating is 366.  I've been capturing 44 inverters for ~100days and have seen 366W only 9 times.
I have many exceedance data points though, so the inverters do better than spec (27,2230 of 500,000 data points are over 349W).
I also calculate the estimated peak power output from the parabola, and the SHAVED energy, which is the difference between 
the estimated power and the generated power (integrated over time).  This gives you an idea of how much energy 
is lost due to undersized inverters.

In addition to the report, you can also view the plotted output by selecting the -PlotType parameter.  Here is an example:
![Plot](Example.png)
As you can see, this particular panel is shaded in the early morning, but it comes on strong with an estimated peak power of
373W (its a 430W STC/327W NMOT panel in the SFBay area).  It lost <1% due to shaving on this day.  
After two passes of cloud removal (the silver and gray lines) the green Best Fit line does an excellent job of tracking the output.

NOTE: depending on your PlotType and PlotLimit criteria, you may get many plots.  After viewing (or saving) a plot, just press 
**q** or click the close window control and the next plot will appear.  If you have too many plots go to the python cmd window and press CTRL-C
and it will stop (though you may have to move your cursor to the current plot first).  Using -PlotMode SHAVED -PlotLimit 10 will only 
plot inverter-days that shave at least 10WHrs of energy.  Use -help to see all available arguments.

The program also produces a summary:
```console
python inverter_analyzer.py -MaxContinuous 349 -DBHost NAS2 -DBPort 3307 -DBUsername enphase -DBPassword redacted -DBDatabase Enphase 2> report.err
Processed 102 days of data for 44.0 inverters with a total output of 9,454,863.38Whr.
Average generated power per day: 92,694.74Whr (2,106.70Whr per inverter)
Maximum inverter power: 2,994.13Whr (by SN542341021944 on 2025-05-22)
Total exceedance power: 12,203.96Whr
Maximum exceedance power: 35.26Whr (by SN542341021917 on 2025-05-22)
Total shaved power: 1,217.51Whr
Maximum shaved power: 17.58Whr (by SN542341021158 on 2025-04-12)
Shave ratio: 0.01% (total shaved power / total generated power)
```
