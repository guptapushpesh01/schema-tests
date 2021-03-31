package com.pushpesh.schematests;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import org.apache.avro.Schema;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class SchemaTests {

    public static void main(String[] args) throws JsonProcessingException {

        String destinationSchema  = "{\"type\":\"record\",\"name\":\"FirstName\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"first_name\",\"type\" : [ \"null\", \"string\" ],\"default\" : null}, {\"name\": \"last_name\", \"type\" : [ \"null\", \"string\" ], \"default\" : null}]}";
        //String destinationSchema = "{ \"type\": \"record\", \"name\": \"FirstName\", \"namespace\": \"com.example\", \"fields\": [{ \"name\": \"first_name\", \"type\": \"string\" }]}";
        String jsonText = "{\"first_name\" : \"pushpesh\"}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = new JsonFactory();
        AvroMapper avroMapper = new AvroMapper();

        //Schema destSchema = new Schema.Parser().setValidate(true).parse(destinationSchema);
        try {
            Schema destSchema = new Schema.Parser().parse(destinationSchema);
            JsonNode jsonNode = mapper.readTree(jsonText);
            ObjectWriter writer = mapper.writer();
            byte[] bytes = writer.writeValueAsBytes(jsonNode);
            ByteArrayInputStream bInput = new ByteArrayInputStream(bytes);
            ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
            IOUtils.copy(bInput, bOutput);

            FormatSchema formatSchema = new FormatSchema() {
              @Override
              public String getSchemaType() {
                return "json";
              }
            };

            JsonFactory  jsonFactory = new JsonFactory();
            JsonGenerator jsonGenerator = jsonFactory.createGenerator(bOutput);
            jsonGenerator.setSchema(formatSchema);


//          ByteArrayInputStream bInput = new ByteArrayInputStream(bytes);
//          ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
//          IOUtils.copy(bInput, bOutput);
//
//            Schema destSchema = new Schema.Parser().parse(destinationSchema);
//            AvroSchema schema = avroMapper.schemaFor(destSchema.getClass());
//            AvroFactory  avroFactory = new AvroFactory();
//            AvroGenerator avroGenerator = avroFactory.createGenerator(bOutput, JsonEncoding.UTF8);
//
//            avroGenerator.setSchema(schema);
//            avroMapper.writeTree(avroGenerator, jsonNode);

        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        try {
//            JsonNode jsonNode = mapper.readTree(jsonText);
//            JsonGenerator generator = factory.createGenerator(
//                    new File("output.json"), JsonEncoding.UTF8).setSchema(formatSchema);
//            mapper.writeTree(generator, jsonNode);
//        } catch (Exception e) {
//            e.printStackTrace();


        }

    }
