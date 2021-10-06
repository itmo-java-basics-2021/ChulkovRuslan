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

public class SegmentImpl implements Segment
{
    private final String _segmentName;
    private final Path _segmentRootPath;
    private final SegmentIndex _segmentIndex;

    private DatabaseOutputStream DataWriter;
    private DatabaseInputStream DataReader;
    private WritableDatabaseRecord rec;

    private boolean isFull = false;
    private final int maxSizeSegment = 100000;
    private long size = 0L;

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException
    {
        Path segmentRootPath = tableRootPath.resolve(segmentName);
        try
        {
            Files.createFile(segmentRootPath);
        }
        catch (IOException e)
        {
            throw new DatabaseException("Cannot create segment", e);
        }

        return new SegmentImpl(segmentName, segmentRootPath);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private SegmentImpl(String name, Path segmentRootPath)
    {
        _segmentName = name;
        _segmentRootPath = segmentRootPath;
        _segmentIndex = new SegmentIndex();

    }

    @Override
    public String getName() {
        return _segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException
    {
        if (isReadOnly())
            return false;

        try(var outputStream = new FileOutputStream(_segmentRootPath.toString(), true))
        {
            var rec = new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);

            _segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
            DataWriter = new DatabaseOutputStream(outputStream);
            size += DataWriter.write(rec);
            if (size >= maxSizeSegment)
                isFull = true;
        }
        catch (IOException e)
        {
            throw new IOException("Cannot write in file", e);
        }

        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException
    {
        try(var inputStream = new FileInputStream(_segmentRootPath.toString()))
        {
            DataReader = new DatabaseInputStream(inputStream);
            Optional<SegmentOffsetInfo> offset = _segmentIndex.searchForKey(objectKey);

            if (offset.isEmpty() || offset.get().getOffset() == -1)
                return Optional.empty();

            //DataReader.skip(offset.get().getOffset());
            inputStream.skip(offset.get().getOffset());
            Optional<DatabaseRecord> value = DataReader.readDbUnit();

            if (value.isEmpty() || !value.get().isValuePresented())
            {
                return Optional.empty();
            }

            return Optional.ofNullable(value.get().getValue());
        }
        catch (IOException e)
        {
            throw new IOException("Cannot read file", e);
        }
    }

    @Override
    public boolean isReadOnly() {
        return isFull;
    }

    @Override
    public boolean delete(String objectKey) throws IOException
    {
        if (isReadOnly())
            return false;

        try(var outputStream = new FileOutputStream(_segmentRootPath.toString(), true))
        {
            DataWriter = new DatabaseOutputStream(outputStream);

            //rec = RemoveDatabaseRecord.create(objectKey.getBytes(StandardCharsets.UTF_8));
            rec = new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8));

            _segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(-1));
            size += DataWriter.write(rec);
            if (size >= maxSizeSegment){
                isFull = true;
            }
        }
        catch (IOException e)
        {
            throw new IOException("Cannot delete from file", e);
        }

        return true;
    }
}
