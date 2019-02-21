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

import static com.google.common.primitives.Ints.toByteArray;

import static com.hotels.road.onramp.api.Utils.murmur2;
import static com.hotels.road.onramp.api.Utils.toPositive;

import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.function.Function;

import lombok.Getter;
import lombok.NonNull;

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
import com.hotels.road.rest.model.RoadType;

public class OnrampImpl implements Onramp {

  private final @NonNull @Getter Road road;
  private final @NonNull OnrampSender sender;
  private final @NonNull Random random;

  private final int partitions;
  private final Function<String, byte[]> keyEncoder;
  private final Function<JsonNode, byte[]> valueEncoder;
  private final Function<JsonNode, JsonNode> partitionNodeFunction;

  public OnrampImpl(Road road, OnrampSender sender) {
    this(road, sender, new Random());
  }

  public OnrampImpl(Road road, OnrampSender sender, Random random) {
    this.road = road;
    this.sender = sender;
    this.random = random;

    partitions = sender.getPartitionCount(road);
    keyEncoder = key -> key == null ? null : key.getBytes(UTF_8);
    valueEncoder = SchemaVersion.latest(road.getSchemas().values())
        .map(schema -> (Function<JsonNode, byte[]>) new AvroJsonEncoder(schema) )
        .orElse(this::noSchemaEncode);

    partitionNodeFunction = Optional
        .ofNullable(road.getPartitionPath())
        .filter(p -> !p.isEmpty())
        .map(KeyPathParser::parse)
        .<Function<JsonNode, JsonNode>> map(PartitionNodeFunction::new)
        .orElse((JsonNode a) -> null);
  }

  private byte[] noSchemaEncode(JsonNode json) {
    if (json == null) {
      return null;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Future<Boolean> sendOnMessage(OnMessage onMessage, Instant time) {
    long now = time.toEpochMilli();
    try {

      // The following permutations are allowed:
      // nullable: null or not null
      //
      // Type of Road | partition | key      | message
      // -------------|-----------|----------|----------
      //  NORMAL      | nullable  | nullable | not null
      //  COMPACT     | null      | not null | nullable

      if (road.getType() == RoadType.NORMAL) {
        if (onMessage.getMessage() == null) {
          throw new InvalidEventException("OnMessage must contain a message");
        }
      }
      else if (road.getType() == RoadType.COMPACT){
        if (onMessage.getPartition() != null) {
          throw new InvalidEventException("Compact roads cannot specify partition");
        }
        if (onMessage.getKey() == null) {
          throw new InvalidEventException("Compact roads must specify a key");
        }
      }

      try {
        int partition = calculatePartition(onMessage);
        byte[] key = keyEncoder.apply(onMessage.getKey());
        byte[] message = valueEncoder.apply(onMessage.getMessage());
        InnerMessage innerMessage = new InnerMessage(partition, now, key, message);

        return sender.sendInnerMessage(road, innerMessage);
      } catch (JasvornoConverterException e) {
        throw new InvalidEventException(e.getMessage());
      }
    } catch (InvalidEventException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  private int calculatePartition(OnMessage onMessage) throws InvalidEventException {

    Integer partition = onMessage.getPartition();
    // TODO: COMPACTED throw and out of range exception for partition greater than available
    if (partition != null) {
      if (partition >= partitions) {
        throw new InvalidEventException(
            String.format("OnMessage with partition id %d exceeds range [0-%d] for road %s",
                partition, partitions - 1, road.getName()));
      }
      else {
        return onMessage.getPartition();
      }
    }

    if (onMessage.getKey() != null) {
      return onMessage.getKey().hashCode() % partitions;
    }

    if (road.getPartitionPath() == null || road.getPartitionPath().isEmpty()) {
      return random.nextInt(partitions);
    }

    JsonNode partitionValue = partitionNodeFunction.apply(onMessage.getMessage());

    if (partitionValue == null || partitionValue.isMissingNode()) {
      return random.nextInt(partitions);
    }

    // This repeats what Data Highway and Kafka together were doing to calculate partitions before we pulled this up to
    // the business logic layer.
    // Previously DH took the hashCode of the partition path value and converted it to a 4 byte array passing the result
    // as the key to Kafka. Kafka would then take the murmur2 hash value of those bytes and mask off the top bit to keep
    // the result positive. Finally Kafka would then take the modulus of the number of partitions.
    return toPositive(murmur2(toByteArray(partitionValue.hashCode()))) % partitions;
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
