package com.xavient.hdfshbase.tesseract;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

public class ExtractData {
    
    final static Logger logger = LoggerFactory.getLogger(ExtractData.class);

    /*
     * private void checkTesseractData(String path) throws HdfsHbaseException {
     * File file = new File(path); if (file.exists()) {
     * 
     * } else { downloadFile(
     * "https://codeload.github.com/tesseract-ocr/tessdata/zip/master",
     * "D:/setup/runnable/tesseract"); } }
     * 
     * private void downloadFile(String urlStr, String destinationDir) throws
     * HdfsHbaseException { try { URL url = new URL(urlStr); File destFile = new
     * File(destinationDir); FileUtils.copyURLToFile(url, destFile); } catch
     * (IOException e) { throw new HdfsHbaseException(e); } }
     */

    public String extractdatates(String args) {
	File imageFile = new File(args);
	ITesseract instance = new Tesseract();
	try {
	    String result = instance.doOCR(imageFile);
	    logger.debug("Parsed result: {}", result);
	    return result;
	} catch (TesseractException e) {
	    System.err.println(e.getMessage());
	}
	return null;
    }
}
