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

import java.util.concurrent.Future;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import com.google.common.util.concurrent.Futures;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.model.core.Road;
import com.hotels.road.onramp.api.OnrampSender;
import com.hotels.road.model.core.InnerMessage;

@Component
@RequiredArgsConstructor
public class MemoryOnrampSender implements OnrampSender {

  private final MemoryRoadPersistence memoryRoadPersistence;

  @Override
  public Future<Boolean> sendInnerMessage(Road road, InnerMessage message) {
    if (message.getPartition() != 0) {
      return Futures.immediateFailedFuture(new InvalidEventException("Partition should be 0"));
    }
    memoryRoadPersistence.write(road.getName(), // normally we use road.getTopicName(), for test purpose only getName.
        message.getPartition(),
        message.getTimestampMs(),
        message.getKey(),
        message.getMessage());
    return Futures.immediateFuture(true);
  }

  @Override
  public int getPartitionCount(Road road) {
    return 1;
  }
}
