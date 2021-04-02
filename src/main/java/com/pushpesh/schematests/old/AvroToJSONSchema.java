package com.pushpesh.schematests.old;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.avro.Avro2JsonSchemaProcessor;
import com.github.fge.avro.Avro2JsonSchemaProcessor.*;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ConsoleProcessingReport;
import com.github.fge.jsonschema.core.report.DevNullProcessingReport;
import com.github.fge.jsonschema.core.report.ListProcessingReport;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.core.tree.JsonTree;
import com.github.fge.jsonschema.core.tree.SchemaTree;
import com.github.fge.jsonschema.core.tree.SimpleJsonTree;
import com.github.fge.jsonschema.core.util.ValueHolder;

import java.io.IOException;

public class AvroToJSONSchema {
  public static void main(String[] args) throws ProcessingException {
    String sourceSchemaStr =
            "{ \"type\": \"record\", \"name\": \"FirstName1\", \"namespace\": \"com.example\", \"fields\": [{ \"name\": \"first_name\", \"type\": \"string\" }]}";
    String sourceSchema2 = "{ \"type\": \"record\", \"name\": \"FirstName1\", \"namespace\": \"com.example\", \"fields\": [{ \"name\": \"first_name\", \"type\": \"string\" }, { \"name\": \"last_name\", \"type\": \"string\", \"default\": \"empty\" }] }";

    Avro2JsonSchemaProcessor processor = new Avro2JsonSchemaProcessor();
    // ProcessingReport report = new DevNullProcessingReport();
    ProcessingReport report = new ConsoleProcessingReport();

    JsonNode avro = null;
    try {
      avro = JsonLoader.fromString(sourceSchema2);
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueHolder<JsonTree> input = ValueHolder.<JsonTree> hold(new SimpleJsonTree(avro));
    ValueHolder<SchemaTree> jsonSchema = processor.process(report, input);
    System.out.println(jsonSchema.getValue().getBaseNode().toString());
  }
}
