package com.pushpesh.schematests;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SchemaTest2 {
    public static void main(String[] args) {
        String destinationSchema  = "{\"type\":\"record\",\"name\":\"FirstName\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"first_name\",\"type\" : [ \"null\", \"string\" ],\"default\" : null}, {\"name\": \"last_name\", \"type\" : [ \"null\", \"string\" ], \"default\" : null}]}";
        String jsonText = "{\"first_name\" : \"pushpesh\"}";
        Schema destSchema = new Schema.Parser().setValidate(true).parse(destinationSchema);
        try {
            convertToJsonString(destSchema);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static <T extends GenericRecord> String convertToJsonString(Schema event) throws IOException {

            String jsonstring = "{\"first_name\" : \"pushpesh\"}";

            try {
            DatumWriter<Schema> writer = new GenericDatumWriter<Schema>(event);
            OutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(event, out);
            writer.write(event, encoder);
            encoder.flush();
            jsonstring = out.toString();
            } catch (IOException e) {
                System.out.println("IOException occurred."+ e);
                throw e;
            }

            return jsonstring;
            }

}