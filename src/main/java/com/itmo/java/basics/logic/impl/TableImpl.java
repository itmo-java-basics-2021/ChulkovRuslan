package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

public class TableImpl implements Table {
    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (tableIndex == null){
            throw new DatabaseException("tableIndex is null");
        }

        if(pathToDatabaseRoot == null){
            throw new DatabaseException("path to database is null");
        }

        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex)
    {
        _name = tableName;
        _tableRootPath = pathToDatabaseRoot.resolve(_name);
        _tableIndex = tableIndex;
    }

    private String _name;
    private Path _tableRootPath;
    private TableIndex _tableIndex;
    private ArrayList<String> _segmentsName = new ArrayList<>();
    //private ArrayList<Segment> _segments;
    //private String _lastSegmentName;
    private Segment _lastSegment;

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException{

        if(_lastSegment == null || _lastSegment.isReadOnly())
        {
            createSegment(objectKey, objectValue);
        }
        try
        {
            _lastSegment.write(objectKey, objectValue);
        }
        catch (IOException e)
        {
            throw new DatabaseException("Cannot write in segment", e);
        }

        _tableIndex.onIndexedEntityUpdated(objectKey, _lastSegment);
    }

    private void createSegment(String objectKey, byte[] objectValue) throws DatabaseException{
        String LastSegmentName = SegmentImpl.createSegmentName(_name);
        Segment segment = SegmentImpl.create(LastSegmentName, _tableRootPath);
        try {
            segment.write(objectKey, objectValue);
        } catch (IOException e) {
            throw new DatabaseException("Cannot write in segment", e);
        }

        _lastSegment = segment;
        _segmentsName.add(segment.getName());
        _tableIndex.onIndexedEntityUpdated(objectKey, segment);
    }


    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        try{
            Optional<Segment> segment = _tableIndex.searchForKey(objectKey);
            if (!segment.isPresent()) {
                return Optional.empty();
            }
            else{
                return segment.get().read(objectKey);
            }
        } catch (IOException e) {
            throw new DatabaseException("Cannot read from file");
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException{
        Optional<Segment> segment = _tableIndex.searchForKey(objectKey);
        if (segment.equals(Optional.empty())){
            throw new DatabaseException("Value by key not found");
        }
        try{
            if(_lastSegment.isReadOnly()) {
                String LastSegmentName = SegmentImpl.createSegmentName(_name);
                Segment segmentNew = SegmentImpl.create(LastSegmentName, _tableRootPath);

                _lastSegment = segmentNew;
                _segmentsName.add(segmentNew.getName());
                _tableIndex.onIndexedEntityUpdated(objectKey, null);
                segmentNew.delete(objectKey);
            }
            else
            {
                segment.get().delete(objectKey);
            }

        } catch (IOException e){
            throw new DatabaseException("Cannot delete from file", e);
        }
    }
}
