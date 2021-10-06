package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table
{
    private final String _name;
    private final Path _tableRootPath;
    private final TableIndex _tableIndex;
    private Segment _lastSegment;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException
    {
        if (tableIndex == null)
        {
            throw new DatabaseException("tableIndex is null");
        }

        if(pathToDatabaseRoot == null)
        {
            throw new DatabaseException("path to database is null");
        }

        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex)
    {
        _name = tableName;
        _tableRootPath = pathToDatabaseRoot.resolve(_name);
        _tableIndex = tableIndex;
        _lastSegment = null;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException
    {
        try
        {
            if(_lastSegment == null || _lastSegment.isReadOnly())
            {
                _lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(_name), _tableRootPath);
            }

            _lastSegment.write(objectKey, objectValue);
            _tableIndex.onIndexedEntityUpdated(objectKey, _lastSegment);
        }
        catch (IOException e)
        {
            throw new DatabaseException("Cannot write in segment", e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException
    {
        try
        {
            Optional<Segment> segment = _tableIndex.searchForKey(objectKey);
            if (segment.isPresent())
            {
                return segment.get().read(objectKey);
            }
            else
            {
                return Optional.empty();
            }
        }
        catch (IOException e)
        {
            throw new DatabaseException("Cannot read from file", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException
    {
        try
        {
            if (_lastSegment == null || _lastSegment.isReadOnly())
            {
                _lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(_name), _tableRootPath);
            }

            _lastSegment.delete(objectKey);
        }
        catch(IOException e)
        {
            throw new DatabaseException("Cannot delete value", e);
        }
    }
}
