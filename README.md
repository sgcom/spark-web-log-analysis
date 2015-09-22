### Exploring a public Web log data set using Apache Spark

---

The dataset is available from [The Internet Traffic Archive](http://ita.ee.lbl.gov/index.html).

##### Dataset source: http://ita.ee.lbl.gov/html/contrib/Sask-HTTP.html

##### Dataset description:

This trace contains seven months’ worth of all HTTP requests to the University of Saskatchewan's WWW server. The University of Saskatchewan is located in Saskatoon, Saskatchewan, Canada.

The log contains over **_2,400,000_** lines from **_June to December 1995_** - the early days of the World Wide Web.

#### Log file sample and format:

```
130.54.25.198 - - [01/Jun/1995:00:28:36 -0600] "GET /~macphed/finite/fe_resources/node1.html" 200 9651
128.171.197.73 - - [01/Jun/1995:00:34:50 -0600] "GET /~scottp/hawaii" 200 29106
130.54.25.198 - - [01/Jun/1995:00:35:01 -0600] "GET /~macphed/finite/fe_resources/node59.html" 200 2042
corvas.cts.com - - [01/Jun/1995:00:44:38 -0600] "GET /~macpherc/hibs/hibs.html" 200 6622
```
 - Remote host IP or host name.
 - Next two fields are not used in this particular case.
 - Date and time of access.
 - HTTP request type (GET/POST/...).
 - URL accessed relative to the server root. Also referred as "endpoint".
 - HTTP response status code
 - Content length

---

#### Artifacts

 * [UofS_Logs_1_ETL.ipynb](https://github.com/sgcom/spark-web-log-analysis/blob/master/UofS_Logs_1_ETL.ipynb) - IPython notebook. Parses the raw log file and loads it into Parquet data store for further analysis with Spark SQL and Spark dataframes. [Parquet](https://parquet.apache.org/) is columnar data format, very efficient for analytics.
 * [UofS_Logs_2_Explore.ipynb](https://github.com/sgcom/spark-web-log-analysis/blob/master/UofS_Logs_2_Explore.ipynb) - IPython notebook. Run SparkSQL and dataframes queries to explore the log data.
