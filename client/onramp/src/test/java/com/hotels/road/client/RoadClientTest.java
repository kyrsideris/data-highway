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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.road.rest.model.StandardResponse;

@RunWith(MockitoJUnitRunner.class)
public class RoadClientTest {

  private final StandardResponse response1 = new StandardResponse(1, true, null);
  private final StandardResponse response2 = new StandardResponse(2, true, null);
  private final StandardResponse response3 = new StandardResponse(3, true, null);
  private final List<StandardResponse> responses = Arrays.asList(response1, response2, response3);

  @Test
  public void defaultMethod() {
    @SuppressWarnings("unchecked")
    RoadClient<Integer> underTest = mock(RoadClient.class, CALLS_REAL_METHODS);
    List<Integer> messages = ImmutableList.of(1, 2, 3);
    List<OnrampMessage<Integer>> onrampMessages = messages.stream()
        .map(OnrampMessage::new)
        .collect(Collectors.toList());

    when(underTest.send(onrampMessages)).thenReturn(responses);

    List<StandardResponse> result1 = underTest.sendMessages(messages);

    assertThat(result1.size(), is(3));
    assertThat(result1.get(0), is(response1));
    assertThat(result1.get(1), is(response2));
    assertThat(result1.get(2), is(response3));

    OnrampMessage<Integer> msg1 = onrampMessages.get(0);
    when(underTest.send(Collections.singletonList(msg1))).thenReturn(Collections.singletonList(response1));

    StandardResponse result2 = underTest.send(msg1);

    assertThat(result2, is(response1));
  }

}
