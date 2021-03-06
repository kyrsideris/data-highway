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
package com.hotels.road.offramp.service;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.TextNode;

import com.hotels.road.pii.PiiReplacer;

@RequiredArgsConstructor
public class JsonNodePiiReplacer implements Function<JsonNode, JsonNode> {
  private final PiiReplacer replacer;

  @Override
  public JsonNode apply(JsonNode value) {
    if (value instanceof TextNode) {
      return TextNode.valueOf(replacer.replace(value.asText()));
    } else if (value instanceof BinaryNode) {
      return BinaryNode.valueOf(new byte[0]);
    } else {
      return value;
    }
  }
}
