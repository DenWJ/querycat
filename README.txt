Use LabKey SQL to query a structure (dataset, list, specimens, etc.) in a LabKey server with the API,
and write the results to a local database table.

Usage:
[server url] [username] [password] [labkey path] [labkey schema] [labkey sql query] [jdbcString] [option]

Examples:
"https://www.itntrialshare.org" "yourlabkeyusername@gmail.com" "yourpassword" "/Studies/ITN027AIPUBLIC/Study Data" study "SELECT * FROM ADSTART0" "jdbc:jtds:sqlserver://youroutputdatabase:1433/DBname;instance=;user=dbname;password=dbpass;"

Users (for lookup since most LabKey tables will return users as integers)
"https://www.itntrialshare.org" "/Studies/Experimental Plans" "yourusername@yourdomain.com" "yourpassword" core "SELECT * FROM SiteUsers" "jdbc:jtds:sqlserver://itn-sqldev-01.immunetolerance.org:1433/TrialshareOutput;instance=;user=dbuser;password=dbpass;"


TO-DO:
1. Make SqlWriter non-singleton
2. SQL optimization (paging multiple rows at a time) SqlWriter.update()
3. Re-evaluate open and close connection timings (test against large dataset)
4. Better logging
5. Figure out how to get proper column precision from API call for any query.
