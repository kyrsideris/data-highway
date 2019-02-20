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
package com.hotels.road.loadingbay.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.rest.model.RoadType;

@JsonDeserialize(builder = HiveRoad.HiveRoadBuilder.class)
@lombok.Data
@lombok.Builder
public class HiveRoad {
  private final String name;
  private RoadType type;
  private final String topicName;
  private final Map<Integer, SchemaVersion> schemas;
  private final Destinations destinations;
  private final KafkaStatus status;

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class HiveRoadBuilder {}
}
