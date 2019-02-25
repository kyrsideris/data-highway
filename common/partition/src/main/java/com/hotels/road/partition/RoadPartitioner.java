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
package com.hotels.road.partition;

import static com.google.common.primitives.Ints.toByteArray;

import static com.hotels.road.partition.Utils.murmur2;
import static com.hotels.road.partition.Utils.toPositive;

import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.NonNull;

public class RoadPartitioner {

  private final int partitions;
  private final @NonNull Random random;

  public RoadPartitioner(int partitions) {
    this(partitions, new Random());
  }

  public RoadPartitioner(int partitions, Random random){
    this.partitions = partitions;
    this.random = random;
  }

  public int partitionWithKey(String key) {
    return toPositive(key.hashCode()) % partitions;
  }

  public int partitionRandomly() {
    return random.nextInt(partitions);
  }

  public int partitionWithPartitionValue(JsonNode partitionValue) {
    // This repeats what Data Highway and Kafka together were doing to calculate partitions before we pulled this up to
    // the business logic layer.
    // Previously DH took the hashCode of the partition path value and converted it to a 4 byte array passing the result
    // as the key to Kafka. Kafka would then take the murmur2 hash value of those bytes and mask off the top bit to keep
    // the result positive. Finally Kafka would then take the modulus of the number of partitions.
    return toPositive(murmur2(toByteArray(partitionValue.hashCode()))) % partitions;
  }
}
