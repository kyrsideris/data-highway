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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.primitives.Ints;

import com.hotels.jasvorno.JasvornoConverter;
import com.hotels.road.model.core.SchemaVersion;

public class AvroJsonEncoder implements Function<JsonNode, byte[]> {

  private static final byte MAGIC_BYTE = 0x00;

  private final byte[] version;
  private final Schema schema;
  private final ByteArrayOutputStream buffer;
  private final BinaryEncoder encoder;
  private final DatumWriter<Object> writer;

  public AvroJsonEncoder(SchemaVersion schemaVersion) {
    version = Ints.toByteArray(schemaVersion.getVersion());
    schema = schemaVersion.getSchema();

    buffer = new ByteArrayOutputStream(2048); // does not need to be closed
    encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
    writer = new GenericDatumWriter<>(schema);
  }

  /**
   * Produces message format expected by Confluent platform: {@code <0x00><4 byte schema Id><avro message>}.
   */
  byte[] encode(GenericRecord record) {
    byte[] bytes = null;
    try {
      buffer.write(MAGIC_BYTE);
      buffer.write(version);
      writer.write(record, encoder);
      encoder.flush();
      bytes = buffer.toByteArray();
      buffer.reset();
    } catch (IOException unreachable) {}
    return bytes;
  }

  @Override
  public byte[] apply(JsonNode record) {
    if (record == null || record.isNull()) {
      return null;
    }
    return encode((GenericRecord) JasvornoConverter.convertToAvro(GenericData.get(), record, schema));
  }
}
