#Content Data Store

Content Data Store (CDS) is a system to provide storage facilities to massive data sets is in the form of images, pdfs, documents and scanned documents. This dataset processed, organized and managed by CDS. CDS is a fast data ingestion and fast lookup system on heterogeneous dataset and its content.
Business wants to store heterogeneous data from various sources into Hadoop or NoSQL databases and run analytics on its contents. There is need for a platform that can help to build a scalable and structured data store and here what CDS is meant for.


##CDS Features

-   High Speed Image Store
-   Content Store
-   Real Time Analytics
-   Text detection  & extraction
-   Document Analysis
-   Actions & Decisions


##CDS Prerequisites

The API has been tested on below mentioned HDP 2.5 components:

-   Apache Hadoop 2.7.1.2.4
-   Apache Kafka 0.10.0.1
-   Apache Hbase 2.0.0-SNAPSHOT
-   Apache Solr  6.2.1
-   Apache Maven
-   Java 1.7 or later

##CDS Technology Stack

-   System - HDFS, NoSQL( Hbase),  Apache Hive
-   Scheduling System - Apache Oozie
-   Reporting System - Zeppelin, Custom UI, Webhdf, Hue
-   Indexing- Apache Solr
-   Programming Language - Java
-   IDE - Eclipse
-   Operating System - Centos, Ubuntu

##CDS Architecture

Content Data Store (CDS) harness the powers of Apache Hadoop, Tesseract, NoSQL database and Apache Solr. 
CDS comes along with a automated system where once data is reached to HDFS, the Oozie schedule event run CDS agent to pulls content from image and store images along with its content. Any data file can be processed  and CDS agent automatically detects file format, its size and store into respective database.


-   Ingestion System:- We can use any ingestion system and DiP can be a best fit.
-   Agent : Tesseract, Leptonica, Oozie, Java.
-   Image Store : HDFS, Hbase/MongoDB
-   Indexing: Apache Solr
-   UI : Zeppelin, Hue or any custom UI which can support integration with underlying NoSQL
![alt text](https://github.com/XavientInformationSystems/CDS/blob/master/src/main/resources/images/architecture.png "Architecture")


##STEPS

Keep some text, PDF, PPT, image file somewhere at cluster. Note down the path for the files.
Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "kafka_topic" using the below mentioned command
```./kafka-topics.sh --create --topic kafka_topic --zookeeper zookeeper-server:port --replication-factor 1 --partition 5```

Download the CDS  source code from "https://github.com/XavientInformationSystems/CDS.git" and compile the code using below commands:

### Decompress the zip file.
```unzip cds.zip```

### Compile the code
```cd cds```
```mvn clean package```

Once the code has being successfully compiled, go to the target directory and locate a jar by the name "uber-cds-1.0.0.jar"
###Submit the jar file to Hadoop cluster to start the process with the following set of argument path using below command:
```java -jar uber-cds-0.0.1-SNAPSHOT.jar -s "path to ppt/pdf/image file" -d "path to destination" -f "path to text file" -c "path to core-site.xml" -h "path to hdfs-site.xml"```

Once you have submitted the jar , you can check the metadata of the file stored in Hbase table  by following commands:

### Launch Hbase shell
```hbase shell```

### Scan the Hbase table
```scan table_name```
![alt text](https://github.com/XavientInformationSystems/CDS/blob/master/src/main/resources/images/hbase-1.png "Architecture")


### Finally, Message is reaching to Kafka Consumer as shown in below screenshot

```./kafka-console-consumer.sh --zookeeper 10.5.3.196:2181 --topic mytopic --from-beginning.```
![alt text](https://github.com/XavientInformationSystems/CDS/blob/master/src/main/resources/images/kafka.png "Architecture")

### On consuming message from Kafka Consumer, it will be indexed by Solr, as shown in the following screenshot.
![alt text](https://github.com/XavientInformationSystems/CDS/blob/master/src/main/resources/images/solr.png "Architecture")

## Use Cases

- Healthcare
- E-Government
- Ticket Collection Analysis
- Heterogeneous Sensor Data
- Equipment  Data Store

## Credits
Xavient

## Technical team

- Neeraj Sabharwal
- Mohiuddin Khan Inamdar
- Shobit Agarwal
- Mukesh kumar


