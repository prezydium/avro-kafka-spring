package com.presidium.dev.avro;

import com.presidium.dev.BasicMonster;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {


    public static void main(String[] args) {
        BasicMonster basicMonster = BasicMonster.newBuilder()
                .setName("orc")
                .setAttack(5.5f)
                .setHealthPoints(30)
                .setArmor(4)
                .setLoot(12)
                .build();
        System.out.println(basicMonster);
        writeAvroFile(basicMonster);
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
}
