package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord
{
    private final byte[] _key;

    public RemoveDatabaseRecord(byte[] objectKey) { _key = objectKey; }

    @Override
    public byte[] getKey() { return _key; }

    @Override
    public byte[] getValue() { return null; }

    @Override
    public long size() { return getKeySize() + 2 * Integer.BYTES; }

    @Override
    public boolean isValuePresented() { return false; }

    @Override
    public int getKeySize() { return _key.length; }

    @Override
    public int getValueSize() { return 0; }
}
