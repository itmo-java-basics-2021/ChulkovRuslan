package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database
{
    private final String _name;
    private final Path _databaseRoot;
    private Map<String, Table> _table;

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null)
        {
            throw new DatabaseException("Name is null");
        }

        if (databaseRoot == null)
        {
            throw new DatabaseException("databaseRoot is null");
        }

        return new DatabaseImpl(dbName, databaseRoot);
    }

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException
    {
        _name = dbName;
        _databaseRoot = databaseRoot.resolve(dbName);
        _table = new HashMap<>();

        try
        {
            Files.createDirectory(_databaseRoot);
        }
        catch (IOException e)
        {
            throw new DatabaseException("Cannot create directory for database",e);
        }
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException
    {
        if (tableName == null)
        {
            throw new DatabaseException("TableName is null");
        }

        Path tableRoot = _databaseRoot.resolve(tableName);
        try
        {
            Files.createDirectory(tableRoot);
        }
        catch (IOException e)
        {
            throw new DatabaseException("Cannot create directory table",e);
        }

        _table.put(tableName, TableImpl.create(tableName, _databaseRoot, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException
    {
        if (_table.containsKey(tableName))
        {
            _table.get(tableName).write(objectKey, objectValue);
        }
        else
        {
            throw new DatabaseException("Cannot find table name");
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException
    {
        if (_table.containsKey(tableName))
        {
            return _table.get(tableName).read(objectKey);
        }
        else
        {
            throw new DatabaseException("Table not found");
        }
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException{
        if (_table.containsKey(tableName))
        {
            _table.get(tableName).delete(objectKey);
        }
        else
        {
            throw new DatabaseException("Table not find");
        }
    }
}
