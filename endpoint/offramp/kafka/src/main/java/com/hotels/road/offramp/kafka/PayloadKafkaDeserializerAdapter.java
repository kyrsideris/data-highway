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
package com.hotels.road.offramp.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.utilities.BaseDeserializer;
import com.hotels.road.offramp.utilities.PayloadDeserializer;

public class PayloadKafkaDeserializerAdapter
    implements Deserializer<Payload<byte[]>>, BaseDeserializer<Payload<byte[]>> {

  private final PayloadDeserializer payloadDeserializer = new PayloadDeserializer();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  public Payload<byte[]> deserialize(String topic, byte[] data) {
    return payloadDeserializer.deserialize(topic, data);
  }
}
