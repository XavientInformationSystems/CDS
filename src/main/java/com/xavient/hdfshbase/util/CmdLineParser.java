package com.xavient.hdfshbase.util;

import java.io.Serializable;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.hdfshbase.constants.Constants;
import com.xavient.hdfshbase.exception.HdfsHbaseException;

public class CmdLineParser implements Serializable {

    private static final long serialVersionUID = -156328589607628437L;
    static final Logger logger = LoggerFactory.getLogger(CmdLineParser.class);

    private CommandLine getCommandLine(String[] args) throws HdfsHbaseException {
	CommandLineParser parser = new BasicParser();
	CommandLine cmdLine;
	try {
	    cmdLine = parser.parse(getOptions(), args);
	} catch (ParseException e) {
	    getHelpStackTrace();
	    throw new HdfsHbaseException("Error while parsing the arguments: " + e.getMessage());
	}
	return cmdLine;
    }

    public AppArgs validateArgs(String[] args) throws HdfsHbaseException {
	AppArgs appArgs = new AppArgs();
	CommandLine cmdLine = getCommandLine(args);
	parseAndValidateConfigFile(appArgs, cmdLine);
	return appArgs;
    }

    private void parseAndValidateConfigFile(AppArgs appArgs, CommandLine cmdLine) throws HdfsHbaseException {
	if (cmdLine.hasOption(Constants.SOURCE_PATH)) {
	    appArgs.setSourcePath((cmdLine.getOptionValue(Constants.SOURCE_PATH)));
	}
	if (cmdLine.hasOption(Constants.TEXT_FILE_PATH)) {
	    appArgs.setTxtSourcePath((cmdLine.getOptionValue(Constants.TEXT_FILE_PATH)));
	}
	if (cmdLine.hasOption(Constants.DEST_PATH)) {
	    appArgs.setDestPath((cmdLine.getOptionValue(Constants.DEST_PATH)));
	}
	if (cmdLine.hasOption(Constants.CORE_SITE)) {
	    appArgs.setCoreSite((cmdLine.getOptionValue(Constants.CORE_SITE)));
	}
	if (cmdLine.hasOption(Constants.HDFS_SITE)) {
	    appArgs.setHDFSSite((cmdLine.getOptionValue(Constants.HDFS_SITE)));
	}
    }

    private Options getOptions() {
	Options options = new Options();
	options.addOption("s", Constants.SOURCE_PATH, true, "Image Source Path");
	options.addOption("d", Constants.DEST_PATH, true, "Image/ text Destination Path");
	options.addOption("f", Constants.TEXT_FILE_PATH, true, "Text File Source Path");
	options.addOption("c", Constants.CORE_SITE, true, "Environmental path for core-site.xml");
	options.addOption("h", Constants.HDFS_SITE, true, "Environmental path for hdfs-site.xml");
	return options;
    }

    private void getHelpStackTrace() {
	HelpFormatter formatter = new HelpFormatter();
	formatter.printHelp("HDFS-HBASE", getOptions());
    }

}
