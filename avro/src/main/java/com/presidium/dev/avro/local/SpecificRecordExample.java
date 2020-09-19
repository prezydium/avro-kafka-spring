package com.presidium.dev.avro.local;

import com.presidium.dev.BasicMonster;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {


    public static void main(String[] args) {
        BasicMonster orc = BasicMonster.newBuilder()
                .setName("orc")
                .setAttack(5.5f)
                .setHealthPoints(30)
                .setArmor(4)
                .setLoot(12)
                .build();
        System.out.println(orc);
        writeAvroFile(orc);
        readAvroFile();
    }

    private static void writeAvroFile(BasicMonster basicMonster) {
        DatumWriter<BasicMonster> datumWriter = new SpecificDatumWriter<>(BasicMonster.class);
        DataFileWriter<BasicMonster> dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(basicMonster.getSchema(), new File("specific-monster.avro"));
            dataFileWriter.append(basicMonster);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void readAvroFile() {
        File fileGeneric = new File("specific-monster.avro");
        SpecificDatumReader<BasicMonster> specificDatumReader = new SpecificDatumReader<>(BasicMonster.class);
        DataFileReader<BasicMonster> dataFileReader;
        try {
            System.out.println("Reading record");
            dataFileReader = new DataFileReader<>(fileGeneric, specificDatumReader);
            while (dataFileReader.hasNext()) {
                BasicMonster readMonster = dataFileReader.next();
                System.out.println(readMonster.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
