package com.pushpesh.schematests.latest;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

public class JsonSchemaTest {
  public static void main(String[] args) throws IOException {

    String jsonDestSchemaStr = "{\"$schema\": \"http://json-schema.org/schema#\",\"type\":\"object\",\"properties\":{\"first_name\":{\"type\":\"string\"}},\"additionalProperties\":false}";
    String jsonSourceSchemaStr = "{\"$schema\": \"http://json-schema.org/schema#\",\"type\":\"object\",\"properties\":{\"first_name\":{\"type\":\"string\"}},\"additionalProperties\":false}";
    String jsonText =
            "{\"first_name\" : \"pushpesh\" }";


    ObjectMapper objectMapper = new ObjectMapper();
    InputStream inputStream = new ByteArrayInputStream(jsonText.getBytes(StandardCharsets.UTF_8));

    Schema schemaSource = new Schema.Parser().setValidate(true).parse(jsonSourceSchemaStr);

    JsonFactory jsonFactory = new JsonFactory();
    ObjectWriter writer = objectMapper.writer();
    byte[] bytes = writer.writeValueAsBytes(jsonText);
    ByteArrayInputStream bInput = new ByteArrayInputStream(bytes);
    ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
    IOUtils.copy(bInput, bOutput);

    JsonGenerator generator = jsonFactory.createGenerator(bOutput);

    Schema schemaDest = new Schema.Parser().setValidate(true).parse(jsonDestSchemaStr);
    JsonNode jsonNode = objectMapper.reader(schemaSource).readTree(inputStream);
    OutputStream outputStream = new ByteArrayOutputStream();
    objectMapper.writer(schemaDest).writeValue(outputStream, jsonNode);
  }
}
