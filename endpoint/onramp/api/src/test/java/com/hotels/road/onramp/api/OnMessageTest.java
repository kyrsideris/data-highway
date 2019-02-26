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
package com.hotels.road.onramp.api;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OnMessageTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private final String key = "123";
  private final String msg = "{\"name\": \"message_123\"}";

  @Test
  public void serialisation_test() throws Exception {
    OnMessage onMessage = mapper.readValue(getJsonText(key, msg), OnMessage.class);
    assertThat(onMessage.getKey(), is(key));
    assertThat(onMessage.getMessage(), is(mapper.createObjectNode().put("name", "message_123")));
  }

  @Test
  public void with_null_key() throws Exception {
    OnMessage onMessage = mapper.readValue(getJsonText(null, msg), OnMessage.class);
    assertThat(onMessage.getKey(), is(nullValue()));
    assertThat(onMessage.getMessage(), is(mapper.createObjectNode().put("name", "message_123")));
  }

  @Test
  public void with_null_message() throws Exception {
    OnMessage onMessage = mapper.readValue(getJsonText(key, null), OnMessage.class);
    assertThat(onMessage.getKey(), is(key));
    assertThat(onMessage.getMessage(), is(nullValue()));
  }

  private String getJsonText(String key, String message) {
    return "{\n"
        + "  \"key\": " + (key == null ? "null" : "\"" + key + "\"") + ",\n"
        + "  \"message\": " + message + "\n"
        + "}";
  }
}
