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
package com.hotels.road.kafkarebalancer;

import static java.lang.String.join;
import static java.util.Collections.nCopies;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.PartitionReplica;

public class KafkaRebalancerTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void importWeighBridge() {
    mapper.findAndRegisterModules();

    List<Broker> brokers =
    IntStream.rangeClosed(0, 65)
        .parallel()
        .mapToObj(
          n -> {
            try {
              return mapper.readValue(
                  KafkaRebalancerTest.class.getResourceAsStream(String.format("/kafka-%d.json", n)),
                  Broker.class);
            }
            catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        )
        .collect(Collectors.toList());

    Map<Integer, Long> brokersToSpace = brokers.stream()
        .map( b -> new Tuple<>(b.getId(), getUsedSpace(b)) )
        .collect(Collectors.toMap(t -> t.x, t-> t.y));

    brokersToSpace
        .entrySet()
        .stream()
//        .sorted((Map.Entry.<Integer,Long>comparingByValue().reversed()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new))
        .forEach( (key, value) -> System.out.println(String.format("%3d -> %d GB: %s", key, value, join("", nCopies((int)(value/10), "|")))) );


    Long rankA = brokers.stream().filter(b -> b.getRack().equals("A")).map(this::getUsedSpace).reduce(Long::sum).get();
    Long rankB = brokers.stream().filter(b -> b.getRack().equals("B")).map(this::getUsedSpace).reduce(Long::sum).get();
    Long rankC = brokers.stream().filter(b -> b.getRack().equals("C")).map(this::getUsedSpace).reduce(Long::sum).get();

    System.out.println(
        String.format("\nData in ranks:\nA -> %d GB\nB -> %d GB\nC -> %d GB\n", rankA, rankB, rankC));


    List<PartitionReplica> partitionsOfA = getPartitionsFilteredByRank(brokers, "A");
    List<PartitionReplica> partitionsOfB = getPartitionsFilteredByRank(brokers, "B");
    List<PartitionReplica> partitionsOfC = getPartitionsFilteredByRank(brokers, "C");


    Integer inSyncA = partitionsOfA.stream().map(p -> (p.isInSync() ? 1 : 0)).reduce(Integer::sum).get();
    Integer inSyncB = partitionsOfB.stream().map(p -> (p.isInSync() ? 1 : 0)).reduce(Integer::sum).get();
    Integer inSyncC = partitionsOfC.stream().map(p -> (p.isInSync() ? 1 : 0)).reduce(Integer::sum).get();

    System.out.println(
        String.format("Number of inSyncs:\nA -> %d\nB -> %d\nC -> %d\n", inSyncA, inSyncB, inSyncC));


    Integer leadersA = partitionsOfA.stream().map(p -> (p.isLeader() ? 1 : 0)).reduce(Integer::sum).get();
    Integer leadersB = partitionsOfB.stream().map(p -> (p.isLeader() ? 1 : 0)).reduce(Integer::sum).get();
    Integer leadersC = partitionsOfC.stream().map(p -> (p.isLeader() ? 1 : 0)).reduce(Integer::sum).get();

    System.out.println(
        String.format("Number of partition leaders:\nA -> %d\nB -> %d\nC -> %d\n", leadersA, leadersB, leadersC));

    // Partitions:
    //    ordered by name
    //    ordered by id
    //      print broker A, B or C

    brokers
        .stream()
        .flatMap(b -> b.getLogDirs().stream()
            .flatMap(l -> l.getTopics().stream()
                .flatMap(t -> t.getPartitionReplicas().stream()
                    .map(p -> new Tuple<>(t.getName(), new Tuple<>(p.getPartition(), b.getRack())))
                )
            )
        )
        .collect(Collectors.groupingBy(tup -> tup.x))
        .entrySet()
        .stream()
        .map(e -> new AbstractMap.SimpleEntry<>(
            e.getKey(),
            e.getValue()
                .stream()
                .map(t -> t.y)
                .sorted(Comparator.comparing(t -> t.x))
                .collect(Collectors.toList())
            )
        )
        .sorted(Map.Entry.comparingByKey())
//        .collect(Collectors.toList())
        .collect(Collectors.toList())
//        .stream()
//        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new))
        .forEach(me -> {
          System.out.println(me.getKey());
          me.getValue()
              .stream()
              .collect(Collectors.groupingBy(tuple -> tuple.x))
              .entrySet()
              .stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> e.getValue().stream().map(t -> t.y).collect(Collectors.toList())))
              .entrySet()
              .stream()
              .forEach(e -> {
                String countA = Long.toString(e.getValue().stream().filter(rank -> rank.equals("A")).count());
                String countB = Long.toString(e.getValue().stream().filter(rank -> rank.equals("B")).count());
                String countC = Long.toString(e.getValue().stream().filter(rank -> rank.equals("C")).count());
                System.out.println(String.format("[%3d]\t%3s\t%3s\t%3s",
                    e.getKey(),
                    countA.equals("0") ? " ": countA,
                    countB.equals("0") ? " ": countB,
                    countC.equals("0") ? " ": countC));
              });
        });
  }

  private Long getUsedSpace(Broker b) {
    return ( b.getLogDirs().get(0).getDiskTotal() - b.getLogDirs().get(0).getDiskFree() ) / 1024 / 1024 / 1024;
  }

  private List<PartitionReplica> getPartitionsFilteredByRank(List<Broker> brokers, String rack) {
    return brokers
        .stream()
        .filter(b -> b.getRack().equals(rack))
        .flatMap(b -> b.getLogDirs().stream()
            .flatMap(l -> l.getTopics().stream()
                .flatMap(t -> t.getPartitionReplicas().stream())))
        .collect(Collectors.toList());
  }

  public class Tuple<X, Y> {
    public final X x;
    public final Y y;
    public Tuple(X x, Y y) {
      this.x = x;
      this.y = y;
    }
  }
}
