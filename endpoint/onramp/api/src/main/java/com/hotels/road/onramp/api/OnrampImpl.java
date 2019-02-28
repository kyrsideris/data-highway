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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;

import com.hotels.jasvorno.JasvornoConverterException;
import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.RoadUnavailableException;
import com.hotels.road.model.core.InnerMessage;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.PartitionNodeFunction;
import com.hotels.road.partition.RoadPartitioner;
import com.hotels.road.rest.model.RoadType;

import lombok.Getter;
import lombok.NonNull;

public class OnrampImpl implements Onramp {

  private final @NonNull @Getter Road road;
  private final @NonNull OnrampSender sender;
  private final @NonNull RoadPartitioner partitioner;

  private final Function<String, byte[]> keyEncoder;
  private final Function<JsonNode, byte[]> valueEncoder;
  private final Function<JsonNode, JsonNode> partitionNodeFunction;

  public OnrampImpl(Road road, OnrampSender sender) {
    this(road, sender, new RoadPartitioner(sender.getPartitionCount(road)));
  }

  public OnrampImpl(Road road, OnrampSender sender, RoadPartitioner partitioner) {
    this.road = road;
    this.sender = sender;
    this.partitioner = partitioner;

    keyEncoder = key -> key == null ? null : key.getBytes(UTF_8);
    valueEncoder = SchemaVersion.latest(road.getSchemas().values())
        .map(schema -> (Function<JsonNode, byte[]>) new AvroJsonEncoder(schema))
        .orElseThrow(() -> new IllegalArgumentException());

    partitionNodeFunction = Optional
        .ofNullable(road.getPartitionPath())
        .filter(p -> !p.isEmpty())
        .map(KeyPathParser::parse)
        .<Function<JsonNode, JsonNode>> map(PartitionNodeFunction::new)
        .orElse((JsonNode a) -> null);
  }

  @Override
  public Future<Boolean> sendOnMessage(OnMessage onMessage, Instant time) {

    if (RoadType.NORMAL == road.getType() && onMessage.getMessage() == null) {
      return Futures.immediateFailedFuture(new InvalidEventException("Normal road messages must contain a message"));
    } else if (RoadType.COMPACT == road.getType() && onMessage.getKey() == null) {
      return Futures.immediateFailedFuture(new InvalidEventException("Compact road messages must specify a key"));
    }

    try {
      int partition = calculatePartition(onMessage);
      byte[] key = keyEncoder.apply(onMessage.getKey());
      byte[] message = valueEncoder.apply(onMessage.getMessage());
      InnerMessage innerMessage = new InnerMessage(partition, time.toEpochMilli(), key, message);

      return sender.sendInnerMessage(road, innerMessage);
    } catch (JasvornoConverterException e) {
      return Futures.immediateFailedFuture(new InvalidEventException(e.getMessage()));
    }
  }

  private int calculatePartition(OnMessage onMessage) {

    if (onMessage.getKey() != null) {
      return partitioner.partitionWithKey(onMessage.getKey());
    }

    if (road.getPartitionPath() == null || road.getPartitionPath().isEmpty()) {
      return partitioner.partitionRandomly();
    }

    JsonNode partitionValue = partitionNodeFunction.apply(onMessage.getMessage());

    if (partitionValue == null || partitionValue.isMissingNode()) {
      return partitioner.partitionRandomly();
    }

    return partitioner.partitionWithPartitionValue(partitionValue);
  }

  @Override
  public boolean isAvailable() {
    return road.isEnabled();
  }

  @Override
  public SchemaVersion getSchemaVersion() {
    return SchemaVersion
        .latest(road.getSchemas().values())
        .orElseThrow(() -> new RoadUnavailableException(String.format("Road '%s' has no schema.", road.getName())));
  }

}
