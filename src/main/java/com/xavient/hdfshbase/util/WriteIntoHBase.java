package com.xavient.hdfshbase.util;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;

public class WriteIntoHBase {
    private static org.apache.log4j.Logger log = Logger.getLogger(WriteIntoHBase.class);

    /**
     * This method is used to create Habse connection with the given
     * configuration parameter
     * 
     * @param Take
     *            input as Configuration object
     * @return connection object
     */
    public Connection creatConnection(Configuration config) throws HdfsHbaseException {
	// Configuration config = HBaseConfigUtil.getHBaseConfiguration();
	Connection connection;
	try {
	    connection = ConnectionFactory.createConnection(config);
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
	log.info("Connection with HBASE created Successfully" + connection);
	return connection;
    }

    /**
     * This method is used to Insert data into Hbase when size of input file is
     * less than 10MB
     * 
     * @param Take
     *            input Checksum of file, filepath, filesize and Configuration
     *            Object
     * @throws Exception
     */
    public void insertIntoHbaseSizeLessThan10MB(String checkSum, String filePath, Long fileSize, String textPath, String ext, Configuration config) throws Exception {
	boolean MOBType = true;
	// Configuration config = HBaseConfiguration.create();
	// Configuration config = HBaseConfigUtil.getHBaseConfiguration();
	// HTable hTable = new HTable(config, Constants.tableName);
	// Connection connection = ConnectionFactory.createConnection(config);
	Connection connection = creatConnection(config);

	Table table;
	try {
	    table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME));
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
	Put p = new Put(Bytes.toBytes("row1"));

	try {
	    p.addColumn(Bytes.toBytes(Constants.ROW_CHECKSUM_ID), Bytes.toBytes(Constants.ROW_CHECKSUM_ID), Bytes.toBytes(checkSum));
	    p.addColumn(Bytes.toBytes(Constants.FILE_PROPERTIES), Bytes.toBytes(Constants.FILE_SIZE), Bytes.toBytes(fileSize));
	    p.addColumn(Bytes.toBytes(Constants.FILE_PROPERTIES), Bytes.toBytes(Constants.FILE_PATH), Bytes.toBytes(filePath));
	    p.addColumn(Bytes.toBytes(Constants.MOB_TYPE), Bytes.toBytes(Constants.MOB_TYPE), Bytes.toBytes(MOBType));
	    p.addColumn(Bytes.toBytes(Constants.TEXT_CONTENT), Bytes.toBytes(Constants.TEXT_CONTENT), extractBytesFile(config, textPath));

	    p.addColumn(Bytes.toBytes(Constants.MOB_IMAGE), Bytes.toBytes(Constants.MOB_IMAGE), extractBytes(filePath));
	    p.addColumn(Bytes.toBytes(Constants.FILE_EXT), Bytes.toBytes(Constants.FILE_EXT), Bytes.toBytes(ext));
	    table.put(p);
	    log.info("Data into Hbase table inserted successfully" + "Checksum:" + checkSum + "FileSize" + fileSize + "FilePath" + filePath + "Content Type" + MOBType);
	    HBaseKafkaProducer kafkaProducer = new HBaseKafkaProducer();
	    kafkaProducer.initialize();
	    kafkaProducer.publishMesssage(1, checkSum);
	    HBaseKafkaConsumer kafkaConsumer = new HBaseKafkaConsumer();
	    kafkaConsumer.initialize();
	    kafkaConsumer.consume(filePath, checkSum, config);

	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	} finally {
	    try {
		table.close();
		connection.close();
	    } catch (IOException e) {
		throw new HdfsHbaseException(e.getMessage());
	    }
	    log.info("Connection closed");
	}

    }

    /**
     * This method is used to Extract the input of MOB type
     * 
     * @param Take
     *            input filepath
     */
    public byte[] extractBytes(String imagePath) throws HdfsHbaseException {
	File imageFile = new File(imagePath);
	BufferedImage image;

	try {
	    image = ImageIO.read(imageFile);
	} catch

	(IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	try {
	    ImageIO.write(image, "jpg", outputStream);
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
	return outputStream.toByteArray();
    }

    public byte[] extractBytesFile(Configuration config, String filePath) throws HdfsHbaseException {
	try (FileSystem fs = FileSystem.get(config); FSDataInputStream dataInputStream = fs.open(new Path(filePath))) {
	    BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
	    StringBuilder sb = new StringBuilder(1024);
	    String line = "";
	    while ((line = reader.readLine()) != null) {
		sb.append(line);
	    }
	    reader.close();
	    return sb.toString().getBytes();
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
    }

    /**
     * This method is used to Insert data into Hbase when size of input file is
     * greater than 10MB
     * 
     * @param Take
     *            input Checksum of file, filepath, filesize and Configuration
     *            Object
     * @throws Exception
     */
    public void insertIntoHbaseSizeGreaterThan10MB(String checkSum, String filePath, Long fileSize, String textPath, String ext, Configuration config) throws Exception {
	boolean MOBType = false;
	// Configuration config = HBaseConfigUtil.getHBaseConfiguration();
	// HTable hTable = new HTable(config, Constants.tableName);
	// Connection connection = ConnectionFactory.createConnection(config);
	Connection connection = creatConnection(config);
	Table table;
	try {
	    table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME));

	    // System.out.println(table.getConfiguration().get("fs.defaultFS"));
	    Put p = new Put(Bytes.toBytes("row1"));

	    p.addColumn(Bytes.toBytes(Constants.ROW_CHECKSUM_ID), Bytes.toBytes(Constants.ROW_CHECKSUM_ID), Bytes.toBytes(checkSum));
	    p.addColumn(Bytes.toBytes(Constants.FILE_PROPERTIES), Bytes.toBytes(Constants.FILE_SIZE), Bytes.toBytes(fileSize));
	    p.addColumn(Bytes.toBytes(Constants.FILE_PROPERTIES), Bytes.toBytes(Constants.FILE_PATH), Bytes.toBytes(filePath));
	    p.addColumn(Bytes.toBytes(Constants.MOB_TYPE), Bytes.toBytes(Constants.MOB_TYPE), Bytes.toBytes(MOBType));
	    p.addColumn(Bytes.toBytes(Constants.TEXT_CONTENT), Bytes.toBytes(Constants.TEXT_CONTENT), extractBytesFile(config, textPath));
	    p.addColumn(Bytes.toBytes(Constants.FILE_EXT), Bytes.toBytes(Constants.FILE_EXT), Bytes.toBytes(ext));
	    table.put(p);
	    System.out.println("data inserted successfully");
	    HBaseKafkaProducer kafkaProducer = new HBaseKafkaProducer();
	    kafkaProducer.initialize();
	    kafkaProducer.publishMesssage(1, checkSum);
	    HBaseKafkaConsumer kafkaConsumer = new HBaseKafkaConsumer();
	    kafkaConsumer.initialize();
	    kafkaConsumer.consume(filePath, checkSum, config);
	    /*
	     * String result = extractdata.extractdatates(filePath);
	     * HBaseMetaDataIndexer indexer = new HBaseMetaDataIndexer();
	     * indexer.indexDocument(checkSum, result);
	     */

	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}

	log.info("Data into Hbase table inserted successfully" + "Checksum:" + checkSum + "FileSize" + fileSize + "FilePath" + filePath + "Content Type" + MOBType);

	try {
	    table.close();
	    connection.close();
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}

	log.info("Connection closed");

    }

    /**
     * This method is used to Insert data into Hbase through Kafka
     * 
     * 
     * @param Take
     *            input Checksum of file, result and Configuration Object
     * @throws Exception
     */
    public void insertintoHbaseKafkaConsumer(String checkSum, String result, Configuration config) throws HdfsHbaseException {

	Connection connection = creatConnection(config);
	Table table;
	try {
	    table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME_KAFKA));

	    Put p = new Put(Bytes.toBytes("row1"));
	    p.addColumn(Bytes.toBytes(Constants.ROW_CHECKSUM_ID), Bytes.toBytes(Constants.ROW_CHECKSUM_ID), Bytes.toBytes(checkSum));
	    p.addColumn(Bytes.toBytes(Constants.TESSERACT_TEXT), Bytes.toBytes(Constants.TESSERACT_TEXT), Bytes.toBytes(result));
	    table.put(p);

	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
	log.info("Data into Hbase table inserted successfully");
	try {
	    table.close();
	    connection.close();
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}

	log.info("Connection closed");

    }

}
