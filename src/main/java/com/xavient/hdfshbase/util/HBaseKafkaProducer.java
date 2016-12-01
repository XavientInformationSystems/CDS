package com.xavient.hdfshbase.util;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.xavient.hdfshbase.constants.Constants;

public class HBaseKafkaProducer {
    private static org.apache.log4j.Logger log = Logger.getLogger(HBaseKafkaProducer.class);
    private static KafkaProducer<Integer, String> kafkaProducer;

    private Properties getKafkaProperties() {
	Properties kafkaProps = new Properties();
	kafkaProps.put("bootstrap.servers", "10.5.3.166:6667");
	kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
	kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	kafkaProps.put("request.required.acks", "1");
	log.info("kafka Producer properties successfully set");
	return kafkaProps;
    }

    public void initialize() {
	kafkaProducer = new KafkaProducer<Integer, String>(getKafkaProperties());
	log.info("Producer initilized successfully");
    }

    private ProducerRecord<Integer, String> getProducerRecord(int key, String checksum) {
	ProducerRecord<Integer, String> record = new ProducerRecord<>(Constants.TOPIC, key, checksum);
	return record;
    }

    public void publishMesssage(int key, String checksum) throws InterruptedException, ExecutionException {
	kafkaProducer.send(getProducerRecord(key, checksum));
	Future<RecordMetadata> metadata = kafkaProducer.send(getProducerRecord(key, checksum), new Callback() {
	    @Override
	    public void onCompletion(RecordMetadata metadata, Exception ex) {

		log.info("producer publish msg sucessfully");

		System.out.println(ex.getMessage());
	    }
	});
	System.out.println(metadata.get().topic());
    }

}
