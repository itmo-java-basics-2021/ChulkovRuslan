package com.itmo.java.basics.logic.io;


import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтах используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public int write(WritableDatabaseRecord databaseRecord) throws IOException {
        try
        {
            writeInt(databaseRecord.getKeySize());
            //writeBytes(new String(databaseRecord.getKey(), StandardCharsets.UTF_8));
            write(databaseRecord.getKey());

            if (!databaseRecord.isValuePresented())
            {
                writeInt(-1);
            }
            else
            {
                writeInt(databaseRecord.getValueSize());
                //writeBytes(new String(databaseRecord.getValue(), StandardCharsets.UTF_8));
                write(databaseRecord.getValue());
            }
        }
        catch(IOException e)
        {
            throw new IOException("Failed write to file");
        }

        return (int)databaseRecord.size();
    }


}