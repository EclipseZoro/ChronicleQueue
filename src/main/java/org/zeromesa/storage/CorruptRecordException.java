package org.zeromesa.storage;

public class CorruptRecordException extends RuntimeException{
    public CorruptRecordException(String message) {
        super(message);
    }
}

