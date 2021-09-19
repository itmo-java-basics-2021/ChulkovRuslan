package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

public class SegmentImpl implements Segment {
    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        Path segmentRootPath = tableRootPath.resolve(segmentName);
        try {
            Files.createFile(segmentRootPath);
        } catch (IOException e) {
            throw new DatabaseException("Cannot create segment", e);
        }

        return new SegmentImpl(segmentName, segmentRootPath);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private SegmentImpl(String name, Path segmentRootPath) throws DatabaseException {
        try{
            _segmentName = name;
            _segmentRootPath = segmentRootPath;
            _segmentIndex = new SegmentIndex();

            String segmentFile = _segmentRootPath.toString();
            DataWriter = new DatabaseOutputStream(new FileOutputStream(segmentFile));

        } catch (IOException e) {
            throw new DatabaseException("Cannot find a file");
        }
    }

    private String _segmentName;
    private Path _segmentRootPath;
    private SegmentIndex _segmentIndex;

    private DatabaseOutputStream DataWriter;
    private DatabaseInputStream DataReader;
    private WritableDatabaseRecord rec;

    private boolean isFull = false;
    private final int maxSizeSegment = 10000;

    @Override
    public String getName() {
        return _segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        try
        {
            rec = SetDatabaseRecord.create(objectKey, objectValue);
            DataWriter.write(rec);
            File file = new File(_segmentRootPath.toString());

            _segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(file.length() - rec.size()));

            if (file.length() >= maxSizeSegment)
                isFull = true;

            return true;
        }
        catch (IOException e)
        {
            throw new IOException("Cannot write in file");
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        try
        {
            DataReader = new DatabaseInputStream(new FileInputStream(_segmentRootPath.toString()));
            Optional<SegmentOffsetInfo> offset = _segmentIndex.searchForKey(objectKey);

            if (offset.equals(Optional.empty()))
                throw new IOException("offset not set");

            DataReader.skip(offset.get().getOffset());
            Optional<DatabaseRecord> value = DataReader.readDbUnit();

            if (!value.get().isValuePresented() || value.isEmpty())
            {
                return Optional.empty();
            }
            else
            {
                //String stringValue = new String(value.get().getValue(), StandardCharsets.UTF_8);
                return Optional.ofNullable(value.get().getValue());
            }
        }
        catch (IOException e)
        {
            throw new IOException("Cannot read file");
        }
    }

    @Override
    public boolean isReadOnly() {
        return isFull;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        try
        {
            String segmentFile = _segmentRootPath.toString();
            DataReader = new DatabaseInputStream(new FileInputStream(segmentFile));

            rec = RemoveDatabaseRecord.create(objectKey);

            DataWriter.write(rec);
            File file = new File(_segmentRootPath.toString());

            _segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(file.length() - rec.size()));

            if (file.length() >= maxSizeSegment){
                isFull = true;
            }

            return true;
        }
        catch (IOException e)
        {
            throw new IOException("Cannot delete from file");
        }
    }
}
