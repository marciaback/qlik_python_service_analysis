# qlik_python_service_analysis
This project was developed in Python and BI QlikViewBI, using an Apache server database.

**Python**
The Python script performs a series of operations to analyze data from logs from an Apache server:
- Decompression and Data Loading: The script starts by decompressing the log file in gzip format called 'apache.log.gz' and saving it as 'apache.log'.

- Reading Data: The uncompressed data is read using the Pandas library and is assigned to a DataFrame called 'data'.

- Data Manipulation for Analysis: Manipulations are performed on the data, including the creation of new columns, such as 'endpoint' from the 'request' column, 'browser' from the 'user-agent' column, 'classec' to identify class C IP addresses, 'time' from the 'time' column, 'status_desc' to categorize the status of requests, and year, month and day columns. Additionally, timing analyses are performed to calculate the difference in hours and minutes between the most recent and oldest logs.

- Saving Transformed Data: The data frame resulting from the manipulations is saved as a CSV file called 'apache_bi.csv'.

- Analyzes performed:
   - The 5 logins that made the most requests
   - The 3 services that received the most requests
   - The 10 most used browsers
   - Network addresses (class C) with the highest number of requests
   - The most visited time of the day
   - The time with the largest number of bytes
   - The endpoint with the highest byte consumption
   - Number of bytes per minute and hour
   - Number of users per minute and hour
   - Requests that had a customer error, grouped by error
   - Number of requests that were successful
   - Number of requests that were redirected

The script was useful in extracting valuable insights from Apache server logs.

**QlikView**

The analyses carried out in Python served as the basis for the development of a BI on the QlikView platform.
With this, the analysis can be available for analysis with new data and in a visual way on an easy-to-understand platform.

* This project was prepared in Portuguese.

![image](https://github.com/marciaback/qlik_python_service_analysis/assets/45545675/014c5a84-9625-4a3f-8e42-6d0bb830cb50)


