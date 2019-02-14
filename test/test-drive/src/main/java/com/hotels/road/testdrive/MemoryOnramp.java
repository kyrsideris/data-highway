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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import lombok.RequiredArgsConstructor;

import com.google.common.util.concurrent.Futures;

import com.hotels.road.model.core.Road;
import com.hotels.road.onramp.api.OnrampSender;
import com.hotels.road.onramp.api.SenderEvent;

@RequiredArgsConstructor
public class MemoryOnramp implements OnrampSender {
  private final Map<String, List<SenderEvent>> messages;

  @Override
  public Future<Boolean> sendEvent(Road road, SenderEvent event) {
    checkArgument(event.getPartition() == 0);
    this.messages.computeIfAbsent(road.getName(), name -> new ArrayList<>()).add(event);
    return Futures.immediateFuture(true);
  }

  @Override
  public int getPartitionCount(Road road) {
    return 1;
  }
}
