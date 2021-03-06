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
package com.hotels.road.offramp.model;

import static com.hotels.road.offramp.model.Event.Type.COMMIT_RESPONSE;

import lombok.Data;
import lombok.NonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class CommitResponse implements Event {
  private final Type type = COMMIT_RESPONSE;
  private final @NonNull String correlationId;
  private final boolean success;

  @JsonCreator
  public CommitResponse(@JsonProperty("correlationId") String correlationId, @JsonProperty("success") boolean success) {
    this.correlationId = correlationId;
    this.success = success;
  }
}
