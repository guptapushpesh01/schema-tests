package com.pushpesh.schematests.old;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.deser.AvroReaderFactory;
import com.fasterxml.jackson.dataformat.avro.deser.AvroStructureReader;
import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class SchemaTest3 {

  public static void main(String[] args) throws IOException {
    /***
     String destinationSchemaStr =
           // "{\"type\":\"record\",\"name\":\"FirstName\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"first_name\", \"type\" : [\"null\", \"string\" ], \"default\" : null}, {\"name\": \"last_name\", \"type\" : [\"null\", \"string\" ], \"default\" : null }]}";
            // "{\"type\":\"record\",\"name\":\"FirstName\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"first_name\", \"type\" : [\"null\", \"string\" ], \"default\" : null}]}";
            "{\"type\":\"record\",\"name\":\"FirstName\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"first_name\",\"type\" : [ \"null\", \"string\" ],\"default\" : null}, {\"name\": \"last_name\", \"type\" : [ \"null\", \"string\" ], \"default\" : null}]}";
    String destStr =
            "{ \"type\": \"record\", \"name\": \"FirstName1\", \"namespace\": \"com.example\", \"fields\": [{ \"name\": \"first_name\", \"type\": \"string\"  }, { \"name\": \"last_name\", \"type\": [\"int\",\"string\"]  }]}";
    String sourceSchemaStr =
            "{ \"type\": \"record\", \"name\": \"FirstName1\", \"namespace\": \"com.example\", \"fields\": [{ \"name\": \"first_name\", \"type\": \"string\"}]}";
    ***/

    String jsonDestSchemaStr = "{\"$schema\": \"http://json-schema.org/schema#\",\"type\":\"object\",\"properties\":{\"first_name\":{\"type\":\"string\"}},\"additionalProperties\":false}";
    String jsonSourceSchemaStr = "{\"$schema\": \"http://json-schema.org/schema#\",\"type\":\"object\",\"properties\":{\"first_name\":{\"type\":\"string\"}},\"additionalProperties\":false}";
    String jsonText =
            "{\"first_name\" : \"pushpesh\", \"last_name\" : \"pushpesh\" }";
    //"{\"first_name\" : \"pushpesh\" }";

    InputStream inputStream = new ByteArrayInputStream(jsonText.getBytes(StandardCharsets.UTF_8));
//    ByteArrayOutputStream out = nesw ByteArrayOutputStream();
//    Schema srcschema = new Schema.Parser().parse(sourceSchemaStr);
//
//    GenericData.Record record = new GenericData.Record(srcschema);
//    System.out.println("record : " + record.toString());
//    Schema dstschema = new Schema.Parser().parse(destinationSchemaStr);
//    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<>(dstschema);
//    writer.write(record, EncoderFactory.get().directBinaryEncoder(out, null));
    byte[] somestring = jsonToAvro(jsonText, jsonDestSchemaStr);
    //avroToJson(somestring, sourceSchemaStr);
    System.out.println(avroToJson(somestring, jsonDestSchemaStr));

//    AvroMapper avroMapper = new AvroMapper();
//    AvroSchema sourceSchema = avroMapper.schemaFrom(sourceSchemaStr);
//    AvroSchema destSchema = avroMapper.schemaFrom(destinationSchemaStr);
//    JsonNode jsonNode = avroMapper.reader(sourceSchema).readTree(inputStream);
//    OutputStream outputStream = new ByteArrayOutputStream();
//    avroMapper.writer(destSchema).writeValue(outputStream, jsonNode);
  }

  public static byte[] jsonToAvro(String json, String schemaStr) throws IOException {
    InputStream input = null;
    GenericDatumWriter<GenericRecord> writer = null;
    Encoder encoder = null;
    ByteArrayOutputStream output = null;
    try {
      Schema schema = new Schema.Parser().parse(schemaStr);
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
      input = new ByteArrayInputStream(json.getBytes());
      output = new ByteArrayOutputStream();
      DataInputStream din = new DataInputStream(input);
      writer = new GenericDatumWriter<GenericRecord>(schema);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
      encoder = EncoderFactory.get().binaryEncoder(output, null);
      GenericRecord datum;
      while (true) {
        try {
          datum = reader.read(null, decoder);
        } catch (EOFException eofe) {
          break;
        }
        writer.write(datum, encoder);
      }
      encoder.flush();
      return output.toByteArray();
    } finally {
      try {
        input.close();
      } catch (Exception e) {
      }
    }
  }

  public static String avroToJson(byte[] avro, String schemaStr) throws IOException {
    boolean pretty = false;
    GenericDatumReader<GenericRecord> reader = null;
    JsonEncoder encoder = null;
    ByteArrayOutputStream output = null;
    try {
      Schema schema = new Schema.Parser().parse(schemaStr);
      reader = new GenericDatumReader<GenericRecord>(schema);
      InputStream input = new ByteArrayInputStream(avro);
      output = new ByteArrayOutputStream();
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
      encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
      Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
      GenericRecord datum;
      while (true) {
        try {
          datum = reader.read(null, decoder);
        } catch (EOFException eofe) {
          break;
        }
        writer.write(datum, encoder);
      }
      encoder.flush();
      output.flush();
      return new String(output.toByteArray());
    } finally {
      try {
        if (output != null) output.close();
      } catch (Exception e) {
      }
    }
  }
}