package com.xavient.hdfshbase.util;

import java.io.Serializable;

public class AppArgs implements Serializable {

    private static final long serialVersionUID = 6792599624479386357L;

    private String sourcePath;
    private String txtSourcePath;

    private String destPath;
    private String coreSite;
    private String hdfsSite;

    public String getTxtSourcePath() {
	return txtSourcePath;
    }

    public void setTxtSourcePath(String txtSourcePath) {
	this.txtSourcePath = txtSourcePath;
    }

    public void setSourcePath(String sourcePath) {
	this.sourcePath = sourcePath;
    }

    public String getSourcePath() {
	return sourcePath;
    }

    public void setDestPath(String destPath) {
	this.destPath = destPath;
    }

    public String getDestPath() {
	return destPath;
    }

    public void setCoreSite(String coreSite) {
	this.coreSite = coreSite;
    }

    public String getCoreSite() {
	return coreSite;
    }

    public void setHDFSSite(String hdfsSite) {
	this.hdfsSite = hdfsSite;
    }

    public String getHDFSSite() {
	return hdfsSite;
    }

}
