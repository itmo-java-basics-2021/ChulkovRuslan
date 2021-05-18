package com.itmo.java.basics;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        Path databaseRoot = Paths.get("E:\\IntelliJ IDEA 2020.3.2\\Projects\\databases");

        try {
            String persons = "persons";
            Database db = DatabaseImpl.create(persons, databaseRoot);
            String children = "children", adults = "adults";
            db.createTableIfNotExists(children);
            db.createTableIfNotExists(adults);

            {
                byte[] value = "bigBoy".getBytes(StandardCharsets.UTF_8);
                db.write("children", "Ruslan", value);
                value = "lilsBoy".getBytes(StandardCharsets.UTF_8);
                db.write("children", "Dima", value);
                value = "BIGLILboy".getBytes(StandardCharsets.UTF_8);
                db.write("children", "Alex", value);
                value = "noLimit".getBytes(StandardCharsets.UTF_8);
                db.write("children", "Alex", value);


                Optional<byte[]> nedValue = db.read("children", "Ruslan");
                String answer = new String(nedValue.get(), StandardCharsets.UTF_8);
                answer = (answer.equals("bigBoy") ? "pass" : "faild");
                System.out.println(answer);

                nedValue = db.read("children", "Dima");
                answer = new String(nedValue.get(), StandardCharsets.UTF_8);
                answer = (answer.equals("lilsBoy") ? "pass" : "faild");
                System.out.println(answer);

                db.delete("children", "Dima");

                nedValue = db.read("children", "Dima");
                answer = (nedValue.equals(Optional.empty())) ? "pass" : "fail";
                System.out.println(answer);

                nedValue = db.read("children", "Alex");
                answer = new String(nedValue.get(), StandardCharsets.UTF_8);
                answer = (answer.equals("noLimit") ? "pass" : "faild");
                System.out.println(answer);

                nedValue = db.read("adults", "Alex");
                answer = (nedValue.equals(Optional.empty())) ? "pass" : "fail";
                System.out.println(answer);
            }
            {
                System.out.print("1. ");
                System.out.println(db.getName().equals(persons)? "passed" : "failed");

                byte[] value_1 = "Fomenko".getBytes(StandardCharsets.UTF_8);
                db.write(adults, "Alexander", value_1);

                byte[] value_2 = "Jim".getBytes(StandardCharsets.UTF_8);
                db.write(adults, "Maxim", value_2);

                byte[] value_3 = "Koko".getBytes(StandardCharsets.UTF_8);
                db.write(adults, "Evj", value_3);

                Optional<byte[]> answer_1 = db.read(adults, "Maxim");
                String sanswer_1 = new String(answer_1.get(), StandardCharsets.UTF_8);
                System.out.print("2. ");
                System.out.println(sanswer_1.equals("Jim") ? "passed" : "failed");

                Optional<byte[]> answer_2 = db.read(adults, "Evj");
                String sanswer_2 = new String(answer_2.get(), StandardCharsets.UTF_8);
                System.out.print("3. ");
                System.out.println(sanswer_2.equals("Koko") ? "passed" : "failed");

                db.delete(adults, "Maxim");
                value_2 = "Jims".getBytes(StandardCharsets.UTF_8);
                db.write(adults, "Maxim", value_2);

                Optional<byte[]> answer_4 = db.read(adults, "Maxim");
                String sanswer_4 = new String(answer_4.get(), StandardCharsets.UTF_8);
                System.out.print("3. ");
                System.out.println(sanswer_4.equals("Jims") ? "passed" : "failed");
            }

            Optional<byte[]> nedValue = db.read("children", "Alex");
            String answer = new String(nedValue.get(), StandardCharsets.UTF_8);
            answer = (answer.equals("noLimit") ? "pass" : "faild");
            System.out.println(answer);


        }
        catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
}

