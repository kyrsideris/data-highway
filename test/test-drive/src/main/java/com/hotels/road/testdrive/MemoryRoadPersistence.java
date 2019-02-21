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
package com.hotels.road.testdrive;

import static java.nio.charset.StandardCharsets.UTF_8;

import static com.hotels.road.offramp.model.DefaultOffset.LATEST;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.SchemaProvider;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.utilities.AvroPayloadDecoder;
import com.hotels.road.offramp.utilities.BaseDeserializer;
import com.hotels.road.offramp.utilities.PayloadDeserializer;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class MemoryRoadPersistence {

  private final Map<String, List<Record>> messages;
  private final Map<StreamKey, AtomicInteger> commits;
  private final SchemaProvider schemaProvider;

  private final AvroPayloadDecoder payloadDecoder = new AvroPayloadDecoder();

  private static final Function<byte[], String> keyDeserializer = key -> key == null ? null : new String(key, UTF_8);
  private static final BaseDeserializer<Payload<byte[]>> valueDeserializer = new PayloadDeserializer();

  public void write(String roadName, Integer partition, Long timestamp, byte[] key, byte[] value) {

    long offset = messages.computeIfAbsent(roadName, n -> new ArrayList<>()).size();
    String k = keyDeserializer.apply(key);
    Payload<byte[]> p = valueDeserializer.deserialize(roadName, value);

    Schema schema = schemaProvider.schema(roadName, p.getSchemaVersion());
    JsonNode message = payloadDecoder.decode(schema, p.getMessage());
    Payload<JsonNode> payload = new Payload<>(p.getFormatVersion(), p.getSchemaVersion(), message);
    Record r = new Record(partition, offset, timestamp, k, payload);

    this.messages.computeIfAbsent(roadName, name -> new ArrayList<>()).add(r);
  }

  public Iterable<Record> read(String roadName, String streamName, DefaultOffset defaultOffset)
      throws UnknownRoadException {
    List<Record> roadMessages = messages.computeIfAbsent(roadName, n -> new ArrayList<>());
    if (roadMessages == null) {
      throw new UnknownRoadException("Unknown road: " + roadName);
    }

    StreamKey streamKey = new StreamKey(roadName, streamName);

    AtomicInteger commit = commits.computeIfAbsent(streamKey, k -> new AtomicInteger(-1));

    int offset = 0;
    if (commit.get() == -1) {
      if (defaultOffset == LATEST) {
        offset = roadMessages.size();
        commit.set(offset);
      }
    } else {
      offset = commit.get();
    }

    if (offset < roadMessages.size()) {
      Record record = roadMessages.get(offset);
      offset++;
      commit.set(offset);
      commits.put(streamKey, new AtomicInteger(offset));
      return Collections.singletonList(record);
    }
    return Collections.emptyList();
  }

  public boolean forward(String roadName, String streamName, Map<Integer, Long> offsets) throws UnknownRoadException {
    List<Record> roadMessages = messages.computeIfAbsent(roadName, n -> new ArrayList<>());
    if (roadMessages == null) {
      throw new UnknownRoadException("Unknown road: " + roadName);
    }

    StreamKey streamKey = new StreamKey(roadName, streamName);

    AtomicInteger commit = commits.computeIfAbsent(streamKey, k -> new AtomicInteger(-1));

    Long o = offsets.get(0);
    if (o != null) {
      commit.set(o.intValue());
      commits.put(streamKey, commit);
    }
    return true;
  }

  @Data
  static class StreamKey {

    private final String roadName;
    private final String streamName;
  }
}
