package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private String _key;

    public static WritableDatabaseRecord create(String objectKey){
        return new RemoveDatabaseRecord(objectKey);
    }

    private RemoveDatabaseRecord(String objectKey) {
        _key = objectKey;
    }

    @Override
    public byte[] getKey() {
        return _key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        return _key.length() + 2 * Integer.BYTES;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return _key.length();
    }

    @Override
    public int getValueSize() {
        return 0;
    }
}
