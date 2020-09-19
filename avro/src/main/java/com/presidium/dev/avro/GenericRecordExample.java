package com.presidium.dev.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class GenericRecordExample {

    private static final String SCHEMA_RESOURCE = "avro/basic-monster.avsc";
    private static final String FILE_NAME = "monster.avro";

    public static void main(String[] args) throws IOException, URISyntaxException {
        Schema.Parser parser = new Schema.Parser();
        URL schemaResourceURL = ClassLoader.getSystemResource(SCHEMA_RESOURCE);
        String schemaAsString = Files.readString(Path.of(schemaResourceURL.toURI()));
        Schema schema = parser.parse(schemaAsString);
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);

        recordBuilder.set("name", "Goblin");
        recordBuilder.set("attack", 2.3f);
        recordBuilder.set("health_points", 15);
        recordBuilder.set("armor", 2);
        recordBuilder.set("loot", 3);

        GenericData.Record monster = recordBuilder.build();

        System.out.println(monster);
        writeAvroFile(monster);

        System.out.println("Read from file:");
        System.out.println(readAvroFile());
    }

    private static List<GenericRecord> readAvroFile() throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(FILE_NAME), new GenericDatumReader<>());
        while (dataFileReader.hasNext()) {
            records.add(dataFileReader.next());
        }
        dataFileReader.close();
        return records;
    }

    private static void writeAvroFile(GenericData.Record record) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(record.getSchema(), new File(FILE_NAME));
        dataFileWriter.append(record);
        dataFileWriter.close();
    }
}
