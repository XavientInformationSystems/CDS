package com.xavient.hdfshbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;

public class FileFilter {

    static final Logger logger = LoggerFactory.getLogger(FileFilter.class);

    /**
     * This method basically verified the file format (filter file for further
     * processing
     * 
     * @param Take
     *            input File HDFS path
     * @return true/false
     */
    public boolean filefilter(String filePath) {
	String ext = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
	for (int i = 0; i < Constants.FILE_LIST.length; i++) {
	    if (Constants.FILE_LIST[i].equalsIgnoreCase(ext))
		return true;
	}
	return false;

    }

    public String getFileExtension(String filePath) {
	return filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
    }

    /**
     * This method is used to return the object of FileStatus *
     * 
     * @param Take
     *            input as Configuration object
     * @return fileStatus
     */
    public FileStatus configurationhdfs(Configuration conf, AppArgs appArgs) throws HdfsHbaseException {
	try {
	    FileSystem fs = FileSystem.get(conf);
	    FileStatus fileStatus = fs.getFileStatus(new Path(appArgs.getSourcePath()));
	    logger.info("File Status Sucess");
	    return fileStatus;
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
    }

    /**
     * This method is used to get the file size(in bytes) of the input file in
     * HDFS
     * 
     * @param Take
     *            input as object of filestatus and checksum of the input file
     * @return file size
     */
    public Long getFileSize(FileStatus fileStatus, FileChecksum sourceChecksum) {
	Long size = fileStatus.getLen();
	logger.info("File Size: {}", size);
	return size;
    }

    /**
     * This method is used to get the checksum of the input file in HDFS
     * 
     * @param Take
     *            input as input source file path and Configuration object
     * @return checksum
     */
    public FileChecksum getFileCheckSum(Path sourceFilePath, Configuration conf) throws HdfsHbaseException {
	try {
	    FileSystem fs = FileSystem.get(conf);
	    FileChecksum sourceChecksum = null;
	    sourceChecksum = fs.getFileChecksum(sourceFilePath);
	    logger.info("Checksum of Source File: {}", sourceChecksum.toString());
	    return sourceChecksum;
	} catch (IOException e) {
	    throw new HdfsHbaseException(e.getMessage());
	}
    }

    public static void main(String args[]) throws Exception {
	System.setProperty("HADOOP_USER_NAME", Constants.HADOOP_USERNAME);
	CmdLineParser cmdLineParser = new CmdLineParser();
	final AppArgs appArgs = cmdLineParser.validateArgs(args);
	Configuration conf = HBaseConfigUtil.getHBaseConfiguration(appArgs);
	FileFilter obj = new FileFilter();
	FileStatus fileStatus = obj.configurationhdfs(conf, appArgs);
	FileChecksum sourceChecksum = obj.getFileCheckSum(fileStatus.getPath(), conf);
	CreateMOBTable createTable = new CreateMOBTable();
	WriteIntoHBase writeintohbase = new WriteIntoHBase();
	Path textPath = new Path(appArgs.getTxtSourcePath());
	logger.info("All Required objectes Created Successfully");

	boolean found = obj.filefilter(fileStatus.getPath().toString());
	if (found) {
	    String ext = obj.getFileExtension(fileStatus.getPath().toString());
	    createTable.createmobTable(conf);
	    Long size = obj.getFileSize(fileStatus, sourceChecksum);
	    if (size < Constants.FILE_SIZE_THRESHOLD) {
		// System.out.println("==>Inserying data into hbase less 10 MB====>");
		writeintohbase.insertIntoHbaseSizeLessThan10MB(sourceChecksum.toString(), fileStatus.getPath().toString(), size, textPath.toString(), ext, conf);
	    } else {
		// Configuration conf = HBaseConfigUtil.getHBaseConfiguration();
		FileSystem fs;
		try {
		    fs = FileSystem.get(conf);
		    Path desPath = new Path(appArgs.getDestPath());
		    FileUtil.copy(fs, fileStatus.getPath(), fs, desPath, false, conf);
		    // System.out.println("==>Inserying data into hbase greater 10 MB======>");
		    writeintohbase.insertIntoHbaseSizeGreaterThan10MB(sourceChecksum.toString(), desPath.toString(), size, textPath.toString(), ext, conf);
		} catch (IOException e) {
		    logger.error(e.getMessage());
		    throw new HdfsHbaseException(e.getMessage());
		}
	    }
	} else {
	    logger.info("Particular file format is not suitable for processing");
	    throw new HdfsHbaseException("Please enter suitable format file");
	}
    }
}
