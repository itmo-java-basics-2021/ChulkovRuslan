package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord
{
    private final byte[] _key;
    private final byte[] _value;

    public SetDatabaseRecord(byte[] objectKey, byte[] objectValue)
    {
        _key = objectKey;
        _value = objectValue;
    }
    @Override
    public byte[] getKey(){ return _key; }

    @Override
    public byte[] getValue() { return _value; }

    @Override
    public long size() { return getKeySize() + getValueSize() + 2 * Integer.BYTES; }

    @Override
    public boolean isValuePresented() { return _value != null; }

    @Override
    public int getKeySize() { return _key.length; }

    @Override
    public int getValueSize() { return isValuePresented() ? _value.length : 0; }
}
