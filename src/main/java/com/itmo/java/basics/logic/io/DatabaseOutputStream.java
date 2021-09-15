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
        try{
            int keySize = databaseRecord.getKeySize();
            writeInt(keySize);

            byte[] key = databaseRecord.getKey();
            writeBytes(new String(key, StandardCharsets.UTF_8));
            //writeUTF(new String(key, StandardCharsets.UTF_8));

            int valueSize = databaseRecord.getValueSize();
            writeInt(valueSize);

            byte[] value = databaseRecord.getValue();
            writeBytes(new String(value, StandardCharsets.UTF_8));
            //writeUTF(new String(value, StandardCharsets.UTF_8));
        }
        catch(IOException e){
            throw new IOException("Failed write to file");
        }

        return (int)databaseRecord.size();
    }


}