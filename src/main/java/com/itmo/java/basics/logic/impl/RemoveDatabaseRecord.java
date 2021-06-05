package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private String _key;
    private byte[] _value;

    public static WritableDatabaseRecord create(String objectKey, byte[] objectValue){
        return new RemoveDatabaseRecord(objectKey, objectValue);
    }

    private RemoveDatabaseRecord(String objectKey, byte[] objectValue) {
        _key = objectKey;
        _value = objectValue;
    }

    @Override
    public byte[] getKey() {
        return _key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {
        return _value;
    }

    @Override
    public long size() {
        return _key.length() + _value.length + 12;
    }

    @Override
    public boolean isValuePresented() {
        return !new String(_value, StandardCharsets.UTF_8).equals("NULL");
    }

    @Override
    public int getKeySize() {
        return _key.length();
    }

    @Override
    public int getValueSize() {
        return -1;
        //_value.length;
    }
}
