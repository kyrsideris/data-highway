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
package com.hotels.road.client;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hotels.road.rest.model.StandardResponse;

public interface RoadClient<T> extends AutoCloseable {
  @Deprecated
  /** @deprecated Use {@link #send(OnrampMessage<T> message)} */
  default StandardResponse sendMessage(T message) {
    return send(new OnrampMessage<>(message));
  }

  @Deprecated
  /** @deprecated Use {@link #send(List<OnrampMessage<T>> messages)} */
  default List<StandardResponse> sendMessages(List<T> messages) {
    return send(messages.stream().map(OnrampMessage::new).collect(Collectors.toList()));
  }

  default StandardResponse send(OnrampMessage<T> message) {
    return send(Collections.singletonList(message)).get(0);
  }

  List<StandardResponse> send(List<OnrampMessage<T>> messages);
}
