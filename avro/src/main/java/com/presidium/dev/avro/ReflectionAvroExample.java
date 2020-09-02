package com.presidium.dev.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;

public class ReflectionAvroExample {

    public static void main(String[] args) throws IOException {
        Schema schema = ReflectData.get().getSchema(ReflectedBasicMonster.class);
        System.out.println(schema);
        File file = new File("basic-monster-reflected.avro");
        DatumWriter<ReflectedBasicMonster> writer = new ReflectDatumWriter<>(ReflectedBasicMonster.class);
        DataFileWriter<ReflectedBasicMonster> out = new DataFileWriter<>(writer)
                .setCodec(CodecFactory.deflateCodec(9))
                .create(schema, file);

        out.append(new ReflectedBasicMonster("Troll", 25f, 50, 10, 101));
        out.append(new ReflectedBasicMonster("Dragon", 99.9f, 300, 25, 1_000_000));
        out.close();
    }
}
