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
package com.hotels.road.onramp.kafka;

import static java.util.Optional.ofNullable;

import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.road.model.core.Road;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampImpl;
import com.hotels.road.onramp.api.OnrampSender;
import com.hotels.road.onramp.api.OnrampService;

@Component
public class OnrampServiceImpl implements OnrampService {
  private final Map<String, Road> roads;
  private final OnrampSender sender;

  @Autowired
  public OnrampServiceImpl(@Value("#{store}") Map<String, Road> roads, OnrampSender sender) {
    this.roads = roads;
    this.sender = sender;
  }

  @Override
  public Optional<Onramp> getOnramp(String name) {
    return ofNullable(roads.get(name)).map(road -> new OnrampImpl(road, sender));
  }
}
