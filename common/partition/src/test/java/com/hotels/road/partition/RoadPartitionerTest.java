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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RoadPartitionerTest {

  long seed = Instant.now().toEpochMilli();
  private Random random = new Random(seed);

  private int partitions = 25;
  private RoadPartitioner partitioner = new RoadPartitioner(partitions, random);

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void partitionWithKey() {
    testRunner(
        1000,
        0.1f,
        2048,
        funFromKeyToPartition()
    );
  }

  @Test
  public void partitionWithPartitionValue() {
    testRunner(
        1000,
        0.1f,
        2048,
        funcFromMessageToPartition()
    );
  }

  @Test
  public void partitionRandomly() {
    testRunner(
        1000,
        0.1f,
        2048,
        fromRandomToPartition()
    );
  }

  private IntFunction<Integer> funFromKeyToPartition() {
    return maxSize -> {
      int keyLength = random.nextInt(maxSize) & Integer.MAX_VALUE;
      String key = RandomStringUtils.randomAscii(keyLength);
      return partitioner.partitionWithKey(key);
    };
  }

  private IntFunction<Integer> funcFromMessageToPartition() {
    return maxSize -> {
      int msgLength = random.nextInt(maxSize) & Integer.MAX_VALUE;
      String msg = RandomStringUtils.randomAscii(msgLength);
      JsonNode node = mapper.createObjectNode().put("message", msg);
      return partitioner.partitionWithPartitionValue(node);
    };
  }

  private IntFunction<Integer> fromRandomToPartition() {
    return i -> partitioner.partitionRandomly();
  }

  private void testRunner(
      int samplesPerPartition,
      float errorMargin,
      int maxMsgSize,
      IntFunction<Integer> undertest){

    int sampleSize = partitions * samplesPerPartition;

    List<Integer> counts = IntStream.range(0, sampleSize)
        .parallel()
        .mapToObj(i -> undertest.apply(maxMsgSize))
        .collect(Collectors.groupingBy(i -> i))
        .values()
        .stream()
        .map(List::size)
        .collect(Collectors.toList());

    int margin = (int) Math.ceil(samplesPerPartition * errorMargin);
    int lower = samplesPerPartition - margin;
    int higher = samplesPerPartition + margin;

    counts.forEach(count -> assertThat(count, allOf(greaterThan(lower), lessThan(higher))));

    int diff = counts.stream().reduce(0, (acc, x) -> acc + Math.abs(samplesPerPartition - x));

    assertThat("There was no randomness in the results. Accumulated difference was: " + diff
            + ", and counts were: " + counts.toString(),
        diff,
        not(0));
  }
}
