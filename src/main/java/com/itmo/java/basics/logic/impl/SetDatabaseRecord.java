package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    private String _key;
    private byte[] _value;

    public static WritableDatabaseRecord create(String objectKey, byte[] objectValue){
        return new SetDatabaseRecord(objectKey, objectValue);
    }

    private SetDatabaseRecord(String objectKey, byte[] objectValue) {
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
        return _key.length() + _value.length + 8;
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
        return _value.length;
    }
}
