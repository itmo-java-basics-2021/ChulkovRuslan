package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit(long offset) throws IOException{
        try {
            Optional<DatabaseRecord> rec;
            skip(offset);

            int keySize = readInt();
            String keyString = readUTF();
            int valueSize = readInt();
            String valueString = readUTF();

            rec = Optional.of(SetDatabaseRecord.create(keyString, valueString.getBytes(StandardCharsets.UTF_8)));


            return rec;
        } catch (IOException e){
            throw new IOException("Cannot read");
        }



    }
}
