# Open Source Reader for Cloud Data Access

The Open Source Reader (OSR) for Cloud Data Access (CDA) is a utility to download table data from an Amazon S3 bucket that has been populated by Guidewire's Cloud Data Access application. The utility reads the .parquet files generated by CDA for each table, and can do one of the following:
- Convert them into human-readable .csv files
- Regenerate them into new `.parquet` files or aggregate them into fewer `.parquet` files
- Load the data into a database: SQL Server, Oracle, PostgreSQL

The OSR is also referred to as the CDA Client. The example code can be used to jump-start development of a custom implementation that can store CDA data in other storage formats (e.g., RDBMS Tables, Hadoop/Hive, JSON Files).
Learn more about CDA [here](https://docs.guidewire.com/cloud/cda/banff/index.html).

- - - 
# What's New
<details>
<summary>November 2021</summary>
<dl><dt><tt>Spark JDBC Sink write for "RAW" data output</tt></dt>
<dd>Added a new class - SparkJDBCWriter - which extends JdbcOutputWriter.</dd>
<dd>This class implements the Spark JDBC sink write to the database, and provides significant performance improvements as compared to the prepared statements used in JdbcOutputWriter.</dd>
<dd>The df.collect() activity in JdbcOutputWriter was causing all data from the collapsed dataframe to be written to memory in the Driver node, causing out-of-memory errors for larger tables. This has been corrected.</dd>
</dl>
<dl><dt><tt>Additional config.yaml settings</tt></dt>
<dd>outputSettings | jdbcBatchSize - batch size of database writes - specific to JdbcOutputWriter Raw/Merged only</dd>
<dd>outputSettings | exportTarget - additional option - jdbc_v2 - used for new jdbc sink write to database</dd>
<dd>jdbcV2Connection - connection information for jdbc sink operation</dd>
<dd>performanceTuning | sparkMaster - set to 'local' for running local mode, or 'yarn' for running on EMR</dd>
</dl>
<dl><dt><tt>Items of note</tt></dt>
<dd>"RAW" and "MERGED" settings in config.yaml: when exportTarget is jdbc_v2, set both saveIntoJdbcRaw and saveIntoJdbcMerged to false</dd>
<dd>New "jdbc_v2" exportTarget setting ONLY writes out "RAW" data to the database connection. If "MERGED" data set is needed it will still use the prepared statements.</dd>
<dd>In the current code version, the new JDBC Sink write cannot be used if "MERGED" output is needed. Work is needed to make that an option.</dd>
<dd>When sparkMaster is set to 'yarn', additional performanceTuning options are ignored.</dd>
</dl>
</details>
<details>
<summary>June 2021</summary>
<dl><dt><tt>Jdbc "RAW" data output</tt></dt>
<dd>Added "gwcbi___operation" to the Primary Key constraint for all raw output tables. The Primary Key for RAW output tables is now ("id”, "gwcbi___seqval_hex", "gwcbi___operation").</dd>
<dd>Added Fingerprint and Timestamp folder columns to assist in troubleshooting – "gwcdac__fingerprintfolder" and "gwcdac__timestampfolder". This addition is a breaking change. RAW database structures will need to be cleared out and reloaded.</dd>
</dl>
<dl><dt><tt>Jdbc "MERGED" data output</tt></dt>
<dd>Added "gwcbi___seqval_hex" column to all tables. This addition is a breaking change. MERGED database structures will need to be cleared out and reloaded. The reason for the additional column – we cannot guarantee the order in which the transactions will show up, only that they will show up. The WHERE clause was expanded to make sure the update applied to the MERGED data is newer than the update already in place.</dd>
</dl>
<dl><dt><tt>Large text fields (length greater than 1333)</tt></dt>
<dd>Added a new parameter in the config.yaml file to specify width of large string columns. In the previous code base the larger string fields were hardcoded, and any time a new large string column was added, it required changes to the code and a rebuild.</dd>
<dd>The current known list of "table.column" values that need to be larger are in the sample.config.yaml at the root of the project. See important details in this document under the "Configuration Parameters" section.</dd>
</dl>
<dl><dt><tt>General changes</tt></dt>
<dd>Reorganized handling of internal column removal and addition. Most of this code is in TableReader.scala with additional references in JdbcOutputWriter.scala.</dd>
<dd>Filtered file pulls by *.parquet. Previously the code just looked for all files (*). In the Cortina CDA code release, a new folder is introduced in each timestamp folder - /cda. The introduction of the folder and files within caused errors that have now been fixed.</dd>
<dd>Corrected support for loading into a single database with multiple schemas. Some basic code fixes here, but necessary especially in PostgreSQL and Oracle.</dd>
<dd>Added support for out of range datetime values in the TimestampType fields for SQL Server. Previously, the Spark JDBC dialect code converted such data types to DATETIME. The data type is now forced to DATETIME2. This change is most likely to be a non-breaking change. Additional note: This will be corrected in the Spark libraries at some point - see GitHub link here https://github.com/apache/spark/pull/32655.</dd>
</dl>
</details>

- - - 
# AWS EMR Cluster and Performance Testing
<details>
<summary>Click to expand</summary>
Guidewire has completed some basic performance testing in AWS EMR. The test CDA instance used represented:

- a 4TB source database 
- with around 715 tables containing around 7B rows,
- resulting in about 350GB of parquet files
- each table represented had one Fingerprint folder and one Timestamp folder

- - - 
## EMR Cluster
### Hardware

- **Master Node** 
    - m5.xlarge, 4 vCore, 16 GiB memory, EBS only storage, EBS Storage:64 GiB
- **Core Node**
    - r5.2xlarge, 8 vCore, 64 GiB memory, EBS only storage, EBS Storage:256 GiB
- **Task Node**
    - m5.2xlarge, 8 vCore, 32 GiB memory, EBS only storage, EBS Storage:32 GiB

- - - 
### Performance Benchmarks
#### JDBC V2 Sink ("RAW" Mode only) + Serverless Aurora PostgreSQL

- **Number of tables** - ~715
- **Number of records** - ~7B
- **Spark Driver** - 3 cores, 8GB
- **Spark Executors** - x5, 2 cores 8GB each
- **maxResultSize** - 8GB
- **Database type** - Serverless
    - **ACUs** - 32-64
- **Load time** - 19 Hours

#### JDBC V2 Sink ("RAW" Mode only) + Aurora PostgreSQL on EC2

- **Number of tables** - ~715
- **Number of records** - ~7B
- **Spark Driver** - 3 cores, 8GB
- **Spark Executors** - x5, 2 cores 8GB each
- **maxResultSize** - 8GB
- **Database type** - EC2, Aurora PostgreSQL
    - **EC2 instance size** - db.r5.2xlarge
- **Load time** - 23 Hours
</details>

- - - 
# Overview of OSR
<details>
<summary>Click to expand</summary>
When converting CDA output to `.csv` files, the utility provides the schema for each table in a `schema.yaml` file, and can be configured to put these files into a local filesystem location or another Amazon S3 bucket. When writing to a database, the data can be loaded in "raw" format, with each insert/update/delete recorded from the source system database, or it can be merged into tables that more closely resemble the source system database.

The utility also resumes downloading from the point which it last read up to when rerun, so that new data in the source bucket can be read efficiently and incrementally over multiple runs.
- - - 
## IDE Setup
### Code Structure

- Written in Scala
- Main program starts in `gw.cda.api.CloudDataAccessClient`
- Config, SavePoints, Manifest, TableReader, OutputWriter
</details>

- - - 
## Build the OSR
<details>
<summary>Click to expand</summary>

1. Set up your IDE:
    - Use Java/JDK 8
    - Open project dir with IntelliJ
2. Download the OSR code.
3. Build by executing this command:
~~~~
./gradlew build
~~~~
4. **For Windows only, download additional utilities**: This utility uses Spark, which in turn uses Hadoop to interact with local filesystems. Hadoop requires an additional Windows library to function correctly with the Windows file system.

    1. Create a `bin` folder in the folder that contains the OSR JAR file.
    2. Download the winutils.exe file for Hadoop 2.7 and place it in `bin` folder
       (e.g., [winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-2.7.7/bin)).
    3. Download and install this Visual C++ Redistributable package:
        - [Visual C++ 2010 Redistributable Package (x86)](http://www.microsoft.com/en-us/download/details.aspx?id=5555) if you're on a 32-bit machine
        - [Visual C++ 2010 Redistributable Package (x64)](http://www.microsoft.com/en-us/download/details.aspx?id=14632) if you're on a 64-bit machine.
    4. Before running the utility, set an additional environment variable named `HADOOP_HOME` to the system path to the folder which contains the "bin" folder that contains the winutils.exe executable.<p>For example, if the `winutils.exe` file is located at `C:\Users\myusername\Documents\cloud-data-access-client\bin\winutils.exe`, set the HADOOP_HOME variable as follows: </p>
~~~~
set HADOOP_HOME=C:\Users\myusername\Documents\cloud-data-access-client-demo
~~~~

For more info, see:
-  [Page at cwiki.apache.org](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems)
- [Page at answers.microsoft.com](https://answers.microsoft.com/en-us/insider/forum/insider_wintp-insider_repair/how-do-i-fix-this-error-msvcp100dll-is-missing/c167d686-044e-44ab-8e8f-968fac9525c5?auth=1)
</details>

- - - 
## Run the OSR
<details>
<summary>Click to expand</summary>

1. **Configure S3 authentication**: The utility requires AWS credentials to access the source S3 bucket that contains the table data (and optionally to write to a destination S3 bucket). These must be exported in the command line before running the program. For example, use this command after replacing `<secret key>`, `<access key>`, and `<region name>` with values from a user's credentials file located in `~/.aws/credentials`:
~~~~
export AWS_SECRET_ACCESS_KEY=<secret key> AWS_ACCESS_KEY_ID=<access key> AWS_REGION=<region name>
~~~~

- If you are using awscli with a credentials file and profiles, you will need to use the environment var `AWS_PROFILE`, instead of setting the keys directly. For example:
~~~~
export AWS_PROFILE=myProfile
~~~~
* More information can be found here for setting up [AWS Credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)

2. Download the sample configuration file from the Git repository folder `/src/test/resources/sample_config.yaml` and save under a new name such as `config.yaml`.
3. Configure the `config.yaml` file.
4. Run the utility by executing the jar from the command line with one of these commands:
   <ul><li>If you are running the OSR for the first time (without a `savepoints.json` file from a previous run) or have a large amount of data in the S3 bucket, both reading and writing can take a substantial amount of time depending on your machine. By default, the Java runtime environment [allocates a maximum of 1/4 of the computer's memory](https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gc-ergonomics.html). It may be necessary to increase the memory available to the application for larger amounts of data. For example, run the OSR with an increased maximum memory allocation of 8 GB ("8g") with this command:
~~~~
java -Xmx8g -jar cloud-data-access-client-1.0.jar --configPath "config.yaml"
~~~~
</li>
<li>If you are downloading incremental changes, run the utility with this command, where the option <tt>--configPath</tt> or <tt>-c</tt> designates the path to the configuration file:
    
~~~~
java -jar cloud-data-access-client-1.0.jar --configPath "config.yaml"
~~~~
</li></ul>

- - -
### EMR Execution

- For execution on EMR, set the sparkMaster config option to 'yarn' (without the single quotes)
- Execution against spark-submit with various parameters (your own options may need to vary based on your cluster):
~~~  
spark-submit --deploy-mode cluster --class gw.cda.api.CloudDataAccessClient --jars s3://<cda-reader-jar> --master yarn --conf spark.yarn.maxAppAttempts=1 --conf spark.driver.cores=3 --conf spark.driver.memory=8G --conf spark.executor.instances=5 --conf spark.executor.cores=2 --conf spark.executor.memory=8G --conf spark.dynamicAllocation.enabled=false --conf spark.driver.maxResultSize=8G --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'" --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'" s3://<cda-reader-jar> -c s3://<config.yml>
~~~

- - -
### Tips

- NOTE: if tests fail due to a Spark error (something like a `BindException`),
  [see this link](https://stackoverflow.com/questions/34601554/mac-spark-shell-error-initializing-sparkcontext)
- For AWS credentials issues with Spark/Hadoop,
  [see this link](http://wrschneider.github.io/2019/02/02/spark-credentials-file.html)
- For using Scala test assertions,
  [see this link](http://www.scalatest.org/user_guide/using_matchers)
</details>

- - -
## Configuration Parameters
<details>
<summary>Click to expand</summary>
Configuration parameters are specified through a .yaml file (typically named config.yaml). 
   <p> </p>
<details>
<summary>Click to see the entire config.yaml file</summary>
<p> 
Config parameters are structured in the file as such:

~~~~
sourceLocation:
  bucketName: ...
  manifestKey: ...
outputLocation:
  path: ...
savepointsLocation:
  path: ...
outputSettings:
  tablesToInclude: ...
  saveIntoJdbcRaw: ...
  saveIntoJdbcMerged: ...
  jdbcBatchSize: ...
  exportTarget: ...
  fileFormat: ...
  includeColumnNames: ...
  saveAsSingleFile: ...
  saveIntoTimestampDirectory: ...
  largeTextFields: ...
jdbcV2Connection:
  jdbcUsername: ...
  jdbcPassword: ...
  jdbcUrl: ...
  jdbcSchema: ...
  jdbcSaveMode: ... 
jdbcConnectionRaw:
  jdbcUsername: ...
  jdbcPassword: ...
  jdbcUrl: ...
  jdbcSchema: ...
  jdbcSaveMode: ...  
jdbcConnectionMerged:
  jdbcUsername: ...
  jdbcPassword: ...
  jdbcUrl: ...
  jdbcSchema: ...
  jdbcApplyLastestUpdatesOnly: ...
performanceTuning:
  sparkMaster: ...
  numberOfJobsInParallelMaxCount: ...
  numberOfThreadsPerJob: ...
sparkTuning:
  maxResultSize: ...
  driverMemory: ...
  executorMemory: ...
~~~~

</p>
</details>
<p></p>
<dl>

<dt><tt>sourceLocation</tt></dt>
<dd>Contains the following parameters about the location of the S3 bucket and the manifest.json file:
<dl><dt><tt>bucketName</tt></dt>
<dd>Name of the source S3 bucket to download data from.</dd>
<dt><tt>manifestKey</tt></dt>
<dd>Path to the manifest.json file in the source bucket, from which the utility reads information about each table. For example, <tt>CDA/manifest.json</tt>. By default, CDA creates the manifest.json file at the same level as table-specific folders in S3 bucket.</dd> </dl></dd>
<dt><tt>outputLocation</tt></dt>
<dd>
<dl>
<dt><tt>path</tt></dt>
<dd>Local file system directory to which the csv files will be written. The directory must exist in the local filesystem before the utility can write to it. To write the CSV's to a S3 bucket, simply replace the path with a valid s3 url (e.g. <tt>s3://example-bucket/cda_client_output</tt>). The utility uses the same AWS credentials for reading from the source bucket and for writing to the destination bucket.</dd>
</dl></dd>

<dt><tt>savepointsLocation</tt></dt>
<dd>
<dl><dt><tt>path</tt></dt>
<dd>Local filesystem directory where the savepoints.json file exists. For more information on this file, see the "Savepoints file" section below.</dd></dl></dd>

<dt><tt>outputSettings</tt></dt>
<dd>
<dl><dt><tt>tablesToInclude</tt></dt>
<dd>(Should be blank by default)</dd><dd> A comma delimited list of tables to include. Leave blank or omit to include all tables in the output.  This is for testing or troubleshooting purposes only. In a Production environment there should be no values here. It is for loading one or more tables to test connectivity, reviewing individual tables in a testing scenario.</dd>
<dt><tt>saveIntoJdbcRaw</tt></dt>
<dd>Boolean (defaults to false)</dd><dd>Should be "true" to write data to a database in Raw format (all activities and operations included in the output). </dd>
<dt><tt>saveIntoJdbcMerged</tt></dt>
<dd>boolean (defaults to false)</dd><dd>Should be "true" to write data to a database in Merged format (more closely representing the source system data). </dd>
<dt><tt>jdbcBatchSize</tt></dt>
<dd>long (defaults to 5000)</dd><dd>DB transaction batch size. This parameter is used only with JDBC Raw and Merged mode.</dd>
<dt><tt>exportTarget</tt></dt>
<dd>(defaults to file)</dd><dd>Available export targets are <tt>file</tt> and <tt>jdbc</tt>.</dd>
<dt><tt>fileFormat</tt></dt>
<dd>(defaults to csv)</dd><dd>Available output formats are .csv and .parquet.</dd>
<dt><tt>includeColumnNames</tt></dt>
<dd>Boolean (defaults to false)</dd><dd>Should be "true" to include a row of column names at the top of the csv file for each table, and "false" for no header row.</dd>
<dt><tt>saveAsSingleFile</tt></dt>
<dd>Boolean (defaults to false)</dd><dd>Should be "true" for writing out a single file (.csv or .parquet) per table, and "false" to have multiple/smaller files be written based on SPARK partitioning.</dd>
<dt><tt>saveIntoTimestampDirectory</tt></dt>
<dd>Boolean (defaults to false)</dd><dd>Should be "true" to save the CSV files into a directory with savepoint timestamp (/outputLocation/path/table/timestamp/*.csv), and "false" to save directly into the table directory (/outputLocation/path/table/*.csv).</dd>
<dt><tt>largeTextFields</tt></dt>
<dd>A comma delimited list of <tt>table.column</tt> columns in your target database that can have very large strings and that must allow max length varchar types.</dd>
<dd>If tables in this list does not exist, OSR will create the columns in the list with max length varchar based on target database platform.</dd> 
<dd>If table already exists in the target database, you must also manually ALTER TABLE to expand the column length. Length values you add **must** expand sufficiently for code to pick up the changes and process properly. You **must** use the following length values based on the database type:<dl>
<dt>For Microsoft SQL Server</dt>
<dd>
ALTER TABLE [table]
ALTER COLUMN [column] VARCHAR(MAX) </dd>
<dt>For PostgreSQL</dt>
<dd>
ALTER TABLE [table]
ALTER COLUMN [column] VARCHAR</dd>
<dt>For Oracle</dt>
<dd>
ALTER TABLE [table]
ALTER COLUMN [column] VARCHAR2(32767) // requires MAX_STRING_SIZE Oracle parameter to be set to EXTENDED. </dd>
</dl>  </dd>


<dd>The following lists known <tt>table.column</tt> values that require "largeTextFields" inclusion. Before you run OSR, add this list to the configuration file:  
      cc_outboundrecord.content, cc_contactorigvalue.origval, pc_diagratingworksheet.diagnosticcapture, cc_note.body, bc_statementbilledworkitem.exception, bc_invoicebilledworkitem.exception, pc_outboundrecord.content, pc_datachange.externalreference, pc_datachange.gosu, bc_workflowworkitem.exception</dd>

</dl></dd>

<dt><tt>jdbcV2Connection</tt></dt>
<dd>Optional section
<dl><dt><tt>jdbcUsername</tt></dt>
<dd>User name used to connect to the database. Can be a placeholder value if using windows authentication for database connectivity. </dd>
<dt><tt>jdbcPassword</tt></dt>
<dd>Password used to connect to the database. Can be a placeholder value if using windows authentication for database connectivity.  </dd>
<dt><tt>jdbcUrl</tt></dt>
<dd>Connection string for database connectivity. </dd>
<dt><tt>jdbcSchema</tt></dt>
<dd>Database schema owner designation for tables written to the database. i.e. - 'dbo' is the default for SQL Server, 'public' is the default for PostgreSQL.</dd>
<dt><tt>jdbcSaveMode</tt></dt>
<dd>(defaults to append)</dd><dd>Values <tt>overwrite</tt> or <tt>append</tt>. Recommended to use 'overwrite' when running bulk load else use 'append'. </dd></dl></dd>

<dt><tt>jdbcConnectionRaw</tt></dt>
<dd>Optional section
<dl><dt><tt>jdbcUsername</tt></dt>
<dd>User name used to connect to the database. Can be a placeholder value if using windows authentication for database connectivity. </dd>
<dt><tt>jdbcPassword</tt></dt>
<dd>Password used to connect to the database. Can be a placeholder value if using windows authentication for database connectivity.  </dd>
<dt><tt>jdbcUrl</tt></dt>
<dd>Connection string for database connectivity. </dd>
<dt><tt>jdbcSchema</tt></dt>
<dd>Database schema owner designation for tables written to the database. i.e. - 'dbo' is the default for SQL Server, 'public' is the default for PostgreSQL.</dd>
<dt><tt>jdbcSaveMode</tt></dt>
<dd>(defaults to append)</dd><dd>Values <tt>overwrite</tt> or <tt>append</tt>. When saveIntoJdbcMerged is true, savemode is not relavant. </dd></dl></dd>

<dt><tt>jdbcConnectionMerged</tt></dt>
<dd>Optional section
<dl><dt><tt>jdbcUsername</tt></dt>
<dd>User name used to connect to the database. Can be a placeholder value if using windows authentication for database connectivity. </dd>
<dt><tt>jdbcPassword</tt></dt>
<dd>Password used to connect to the database. Can be a placeholder value if using windows authentication for database connectivity.  </dd>
<dt><tt>jdbcUrl</tt></dt>
<dd>Connection string for database connectivity. </dd>
<dt><tt>jdbcSchema</tt></dt>
<dd>Database schema owner designation for tables written to the database. i.e. - 'dbo' is the default for SQL Server, 'public' is the default for PostgreSQL.</dd>
<dt><tt>jdbcApplyLatestUpdatesOnly</tt></dt>
<dd>Boolean (defaults to false)</dd><dd>Should be "true" for applying the latest version of a record for a given table. "false" will process all the activities for a record in the order they occurred. for CDC processing, the most recent entry for a given record is the current state of that record. this option allows the application of only that most recent activity and version of the record.</dd>
</dl></dd>

<dt><tt>performanceTuning</tt></dt>
<dd>Optional section
<dl><dt><tt>sparkMaster</tt></dt>
<dd>(defaults to 'local')</dd><dd>Values <tt>local</tt> or <tt>yarn</tt>. Use 'yarn' when running the application on AWS EMR else use 'local'. </dd></dl></dd>
<dl><dt><tt>numberOfJobsInParallelMaxCount</tt></dt>
<dd>Integer - defaults to the number of processors on your machine</dd><dd>Depending on your machine/network, you can go to about 2 times that to get more concurrency.</dd>
<dt><tt>numberOfThreadsPerJob</tt></dt>
<dd>integer - defaults to 10</dd><dd>This allows for parallel parquet file downloads while processing a given table.</dd></dl></dd>

<dt><tt>sparkTuning</tt></dt>
<dd>Optional section
<dl><dt><tt>maxResultSize</tt></dt>
<dd>See <a href="https://spark.apache.org/docs/latest/configuration.html#application-properties">spark.driver.maxResultSize</a>. The OSR places no limit on this by default, so you usually don't have to touch it.</dd>
<dt><tt>driverMemory</tt></dt>
<dd>See <a href="https://spark.apache.org/docs/latest/configuration.html#application-properties">spark.driver.memory</a>. Set this to a large value for better performance.</dd>
<dt><tt>executorMemory</tt></dt>
<dd>See <a href="https://spark.apache.org/docs/latest/configuration.html#application-properties">spark.executor.memory</a>.</dd>
</dl></dd>
</dl>

`Warning: Boolean parameters default to "false" if they are not set.`
</details>

- - - 
## More about outputs
### Savepoints file
<details>
<summary>Click to expand</summary>
The OSR creates a savepoints.json file to keep track of the last batch of table data which the utility has successfully read and written. An example of a savepoints file's contents:

~~~~
{ 
  "taccounttransaction": "1562112543749", 
  "taccount": "1562112543749", 
  "note": "1562112543749", 
  "taccountlineitem": "1562112543749", 
  "taccttxnhistory": "1562112543749", 
  "history": "1562112543749" 
}
~~~~

In the source location, each table has a corresponding timestamp. Each table's timestamp corresponds to the timestamped subfolder in the source destination bucket for when it was written by CDA. For example:
~~~~
/history
  /1562111987488 (timestamped subfolder)
    x.parquet
    y.parquet
  /1562112022178
    z.parquet
  ...
~~~~

The utility creates a savepoints file if run initially without any pre-existing savepoints file, during which the utility will consume all available data in the source bucket.

The OSR uses the CDA writer's manifest.json file to determine which timestamp directories are eligible for copying.  For example, if source bucket data exists, but its timestamp has not been persisted by the CDA writer to the manifest.json file, this data will not be copied by the OSR, since it is considered uncommitted.

Each time the utility runs, the utility derives a time range (for each table) of timestampOfLastSavePoint to timestampInManifestJsonFile to determine the files to copy.

There can be multiple source files (based on the multiple timestamp directories), and we will combine them all into 1 CSV when writing the output file.  This will happen since the CDA Writer is writing continuously, which results in a new timestamp directory say every few minutes, but the OSR may only run once daily.  All new timestamp directories (since the last savepoint) will get copied into the 1 CSV file.

To re-run the utility to re-copy all data in the source bucket, simply delete the savepoints file.  Dont forget to first clean your output location in this case.

Each time a table has been copied (read/written) the savepoints file will be updated.  This allows you to stop the utility in the middle while running.  In this case, we recommend looking at the in-flight table copy/jobs output directories before re-starting again.

A note about the savepoints file:  The ability to save to "Raw" database tables, and "Merged" database tables at the same time is allowed. However, only one savepoints file is written per instance of the OSR application.  If either of the output methods fail, the savepoints data will not be written for the table that fails.
</details>

- - -
### CSV output files
<details>
<summary>Click to expand</summary>
The utility writes CSV files to the configured location directory.

The names of these files are randomly generated by the SPARK partition, and look like "part-00000-216b8c03-8c73-47ca-bcb6-0d38aef6be37-c000.csv".  The name does not reflect any chronological importance.  So running the utility over the same data will result in different filenames.  This is why we recommend using the setting "saveIntoTimestampDirectory" to help differentiate data files.

If you set saveAsSingleFileCSV=false, you will get multiple files, they will all be prefixed with "part-00000", "part-00001", "part-00002", etc.
</details>

- - -
### RDBMS Output
<details>
<summary>Click to expand</summary>
Output to standard RDBMS platforms allows for two options: "Raw" and "Merged".

"Raw" output maintains all activities and transactions as seen in the CSV files output. Each Insert, Update, and Delete activity recorded are included in the "Raw" database output, along with the gwcbi___* columns indicating the sequence and operations.

"Merged" output merges the activities of a given record down to a view of the record as it looked at a point in time in the source system database. Instead of inserting each activity, only the latest version of the record exists, making it appear more like the source system database table it represents.

Database permissions for the account running the application _must_ include:
- CREATE TABLE
- ALTER TABLE
- INSERT
- UPDATE
- DELETE

- - -
#### **RDBMS - Column Exclusions**
This version of the OSR excludes certain data types that contain compound attributes due to an inability to properly insert the data into the database.

The current exclusions include columns with these words in the column name:
- spatial
- textdata

- - -
#### **RDBMS - Table changes**

This version of the OSR application supports Limited programmatic table definition changes. If a parquet file structure changes - i.e. - columns have been added in the underlying source system for that table - the application will automatically add any new columns to the existing table via ALTER TABLE statements.

To accomplish this, the ability to run in parallel for fingerprint folders for any given table has been turned off. If there are multiple fingerprint folders in a given load for a given table, only the earliest fingerprint folder will be processed during that run. Additional fingerprint folders will be picked up in subsequent loads.

The application generates a _cdawarnings.log_ log file in the application root directory when:
- Errors are encountered
- A table has multiple fingerprint folders to load, requiring multiple job runs to process them
- ALTER TABLE statements have been executed - the Success or Failure of the execution of those statements and the statement that was generated and executed due to table schema changes will be listed
</details>

- - -
## Example of an OSR run with CSV output
<details>
<summary>Click to expand</summary>

The source bucket is called cda-client-test. Its contents under the directory CDA/SQL include the folder containing the files for each table, as well as the manifest.json file.

![S3 Sample Structure](./images/cda_client_s3_structure.png)

Thus the source bucket as well as the manifest.json location should be configured in the config.yaml file as such:
`
sourceLocation:
  bucketName: cda-client-test
  manifestKey: CDA/SQL/manifest.json
`


In the local filesystem, the OSR jar and config.yaml file exist in the current directory, along with a directory in which to contain the .csv outputs:
~~~~
cloud-data-access-client-1.0.jar
config.yaml
cda_client_output/
~~~~


As a result the config.yaml is configured as such. I'm designating the savepoints file to also be stored in the cda_client_output directory. Here we are also specifying not to include column names in the output csv:
~~~~
outputLocation:
  path: cda_client_output
savepointsLocation:
  path: cda_client_output
outputSettings:
  includeColumnNames: false
  saveAsSingleFileCSV: true
  saveIntoTimestampDirectory: false
~~~~


After exporting my S3 credentials, I run the jar from the current directory with the command

~~~~
java -jar cloud-data-access-client-1.0.jar -c "config.yaml"
~~~~


After the OSR completes writing, the contents of cda_client_output looks like so:

![Sample Output](./images/cda_client_sample_output.png)

Each table has a corresponding folder. The .csv file in a folder contains the table's data, and the schema.yaml contains information about the columns, namely the name, dataType, and nullable boolean for each column.

When rerunning the utility, the OSR will resume from the savepoints written in the savepoints.json file from the previous. The existing .csv file is deleted, and a new .csv file containing new data will be written in its place.
</details>
