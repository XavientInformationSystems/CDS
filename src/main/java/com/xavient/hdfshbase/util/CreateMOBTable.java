package com.xavient.hdfshbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.hdfshbase.exception.HdfsHbaseException;

/**
 * This method is used create HBase Table with MOB type enabled
 * 
 * @param Take
 *            input as Configuration object
 */
public class CreateMOBTable {

    final static Logger logger = LoggerFactory.getLogger(CreateMOBTable.class);

    public void createmobTable(Configuration config) throws HdfsHbaseException {
	Connection conn = null;
	HBaseAdmin admin = null;

	try {
	    logger.info("creating connection: {}", config.get("hbase.zookeeper.property.clientPort"));
	    conn = ConnectionFactory.createConnection(config);
	    admin = (HBaseAdmin) conn.getAdmin();

	    /*
	     * if
	     * (!admin.isTableEnabled(TableName.valueOf(Constants.tableName))) {
	     * 
	     * System.out.println("inserting data"); HTableDescriptor hbaseTable
	     * = new HTableDescriptor(TableName.valueOf(Constants.tableName));
	     * HColumnDescriptor Row_Checksum_id = new
	     * HColumnDescriptor(Constants.Row_Checksum_id); HColumnDescriptor
	     * Mob_Image = new HColumnDescriptor(Constants.Mob_Image);
	     * HColumnDescriptor Content_Type = new
	     * HColumnDescriptor(Constants.MOB_Type); HColumnDescriptor
	     * File_Properties = new
	     * HColumnDescriptor(Constants.File_Properties); HColumnDescriptor
	     * Text_File = new HColumnDescriptor(Constants.Text_Content);
	     * HColumnDescriptor File_Ext = new
	     * HColumnDescriptor(Constants.File_Ext);
	     * 
	     * Mob_Image.setMobEnabled(true);
	     * Mob_Image.setMobThreshold(1000000000L);
	     * hbaseTable.addFamily(Row_Checksum_id);
	     * hbaseTable.addFamily(Mob_Image);
	     * hbaseTable.addFamily(Content_Type);
	     * hbaseTable.addFamily(File_Properties);
	     * hbaseTable.addFamily(Text_File); hbaseTable.addFamily(File_Ext);
	     * 
	     * admin.createTable(hbaseTable);
	     * System.out.println("Table created"); }
	     */

	} catch (IOException e) {
	    logger.error("Got exception: {}", e.getMessage());
	    throw new HdfsHbaseException(e.getMessage());
	} catch (Exception e) {

	    throw new HdfsHbaseException(e.getMessage());
	} finally {
	    try {
		if (admin != null) {
		    admin.close();
		}

		if (conn != null && !conn.isClosed()) {
		    conn.close();
		}
	    } catch (Exception e2) {
		e2.printStackTrace();
	    }
	}
    }
}
