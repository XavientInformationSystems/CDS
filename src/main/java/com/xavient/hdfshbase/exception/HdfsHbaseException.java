package com.xavient.hdfshbase.exception;

public class HdfsHbaseException extends Exception {

    private static final long serialVersionUID = 1138100039870399881L;

    public HdfsHbaseException(String message) {
	super(message);
    }

    public HdfsHbaseException(Throwable throwable) {
	super(throwable);
    }

    public HdfsHbaseException(String message, Throwable throwable) {
	super(message, throwable);
    }

}
