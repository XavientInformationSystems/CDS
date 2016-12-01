package com.xavient.hdfshbase.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import com.xavient.hbase.solr.HBaseMetaDataIndexer;
import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;
import com.xavient.hdfshbase.tesseract.ExtractData;

public class HBaseKafkaConsumer {
    private static org.apache.log4j.Logger log = Logger.getLogger(HBaseKafkaConsumer.class);

    private static KafkaConsumer<String, String> consumer;

    private Properties getConsumerProperties() {
	Properties props = new Properties();
	props.setProperty("bootstrap.servers", "10.5.3.166:6667");
	props.setProperty("group.id", "testgroup");
	props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	log.info("Kafka Consumer propereties successfully set");
	return props;
    }

    /**
     * This method is used to initialize the config property required for the
     * Kafka consumer
     * 
     * 
     *
     * @throws Exception
     */
    public void initialize() {
	consumer = new KafkaConsumer<>(getConsumerProperties());

	consumer.subscribe(Collections.singletonList(Constants.TOPIC));
	log.info("Kafka Consumer initilised successfully");
	// System.out.println("Kafka Consumer initilised successfully");
    }

    /**
     * This method is used to consume message from kafka Consumer
     * 
     * @param Take
     *            input Checksum of file, filepath, filesize and Configuration
     *            Object
     * @throws Exception
     */
    public void consume(String filePath, String checkSum, Configuration config) throws HdfsHbaseException {
	boolean running = true;
	ExtractData extractdata = new ExtractData();
	try {
	    while (running) {
		ConsumerRecords<String, String> records = consumer.poll(1000);
		for (ConsumerRecord<String, String> consumerRecord : records) {
		    System.out.println(consumerRecord.checksum());
		    String result = extractdata.extractdatates(filePath);
		    HBaseMetaDataIndexer indexer = new HBaseMetaDataIndexer();
		    WriteIntoHBase writetohbase = new WriteIntoHBase();
		    writetohbase.insertintoHbaseKafkaConsumer(checkSum, result, config);
		    indexer.indexDocument(checkSum, result);
		}
	    }
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	} catch (SolrServerException e) {
	    throw new HdfsHbaseException(e.getMessage());
	} finally {
	    consumer.close();
	}
    }

}
