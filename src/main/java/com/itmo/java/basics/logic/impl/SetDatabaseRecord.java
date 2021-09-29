package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    //private String _key;
    private byte[] _key;
    private byte[] _value;

    public static WritableDatabaseRecord create(byte[] objectKey, byte[] objectValue){
        return new SetDatabaseRecord(objectKey, objectValue);
    }

    private SetDatabaseRecord(byte[] objectKey, byte[] objectValue) {
        _key = objectKey;
        _value = objectValue;
    }
    @Override
    public byte[] getKey() {
        return _key;
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
        return _key.length;
    }

    @Override
    public int getValueSize() {
        if (isValuePresented())
        {
            return _value.length;
        }
        else
        {
            return 0;
        }
    }
}
