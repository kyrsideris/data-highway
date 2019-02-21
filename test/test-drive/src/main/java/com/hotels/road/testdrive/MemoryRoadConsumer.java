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

import static java.util.Collections.singleton;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import com.hotels.road.offramp.api.Record;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.spi.RoadConsumer;

@AllArgsConstructor
class MemoryRoadConsumer implements RoadConsumer {

  private final MemoryRoadPersistence memoryRoadPersistence;

  private final String roadName;
  private final String streamName;
  private final DefaultOffset defaultOffset;

  @Override
  public void init(long initialRequest, RebalanceListener rebalanceListener) {
    rebalanceListener.onRebalance(singleton(0));
  }

  @Override
  public Iterable<Record> poll() {
    try {
      return memoryRoadPersistence.read(roadName, streamName, defaultOffset);
    } catch (UnknownRoadException e) {
      return Collections.emptyList();
    }
  }

  @Override
  public boolean commit(Map<Integer, Long> offsets) {
    try {
      return memoryRoadPersistence.forward(roadName, streamName, offsets);
    } catch (UnknownRoadException e) {
      return false;
    }
  }

  @Override
  public void close() {}

  @Component
  @RequiredArgsConstructor
  static class Factory implements RoadConsumer.Factory {
    private final MemoryRoadPersistence memoryRoadPersistence;
    private final Map<String, List<Record>> messages;

    @Override
    public RoadConsumer create(String roadName, String streamName, DefaultOffset defaultOffset)
      throws UnknownRoadException {

      if (!messages.containsKey(roadName)) {
        throw new UnknownRoadException("Unknown road: " + roadName);
      }

      return new MemoryRoadConsumer(memoryRoadPersistence, roadName, streamName, defaultOffset);

    }
  }
}
