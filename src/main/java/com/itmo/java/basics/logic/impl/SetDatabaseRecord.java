package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    private String _key;
    //private byte[] _key;
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
        return getKeySize() + getValueSize() + 2 * Integer.BYTES;
    }

    @Override
    public boolean isValuePresented() {
        return _value != null;
    }

    @Override
    public int getKeySize() {
        return _key.length();
    }

    @Override
    public int getValueSize() {
        if (!isValuePresented())
        {
            return 0;
        }
        else
        {
            return _value.length;
        }
    }
}
