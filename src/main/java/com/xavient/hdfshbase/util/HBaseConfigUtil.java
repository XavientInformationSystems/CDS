package com.xavient.hdfshbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfigUtil {
    public static Configuration getHBaseConfiguration(AppArgs appArgs) {
	Configuration conf = HBaseConfiguration.create();
	//
	conf.set("fs.defaultFS", "hdfs://10.5.3.166:8020");
	conf.set("hbase.zookeeper.quorum", "10.5.3.185");
	conf.set("hbase.zookeeper.property.clientPort", "2181");
	conf.set("zookeeper.znode.parent", "/hbase-unsecure");
	conf.set("hbase.cluster.distributed", "true");
	conf.addResource(new Path(appArgs.getCoreSite()));
	conf.addResource(new Path(appArgs.getHDFSSite()));
	return conf;
    }
}