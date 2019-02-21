/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.road.onramp.api;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.jasvorno.JasvornoConverter;

public class AvroBinaryTest {
  ObjectMapper mapper = new ObjectMapper();

  Schema schema_v1 = new Schema.Parser()
      .parse(
          "{\n"
              + "  \"name\": \"v1\",\n"
              + "  \"type\": \"record\",\n"
              + "  \"fields\": [{\n"
              + "    \"name\": \"a\",\n"
              + "    \"type\": \"int\"\n"
              + "  }, {\n"
              + "    \"name\": \"b\",\n"
              + "    \"type\": \"int\"\n"
              + "  }]\n"
              + "}");
  Schema schema_v2 = new Schema.Parser()
      .parse(
          "{\n"
              + "  \"name\": \"v1\",\n"
              + "  \"type\": \"record\",\n"
              + "  \"fields\": [{\n"
              + "    \"name\": \"a\",\n"
              + "    \"type\": \"int\"\n"
              + "  }, {\n"
              + "    \"name\": \"b\",\n"
              + "    \"type\": \"int\",\n"
              + "    \"default\": 1\n"
              + "  }]\n"
              + "}");

  @Test
  public void testName() throws Exception {
    JsonNode json = mapper.readTree("{\n" + "  \"a\": 1,\n" + "  \"b\": 1\n" + "}");
    byte[] avro_v1 = avroToBytes(schema_v1, json);
    byte[] avro_v2 = avroToBytes(schema_v2, json);

    assertThat(Arrays.equals(avro_v1, avro_v2), is(true));
  }

  private byte[] avroToBytes(Schema schema, JsonNode json) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(2048);
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
    DatumWriter<Object> writer = new GenericDatumWriter<>(schema);

    GenericRecord record = (GenericRecord) JasvornoConverter.convertToAvro(GenericData.get(), json, schema);

    writer.write(record, encoder);
    encoder.flush();
    return buffer.toByteArray();
  }
}
