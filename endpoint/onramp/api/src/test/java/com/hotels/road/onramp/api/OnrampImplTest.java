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

import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;

import com.hotels.road.model.core.InnerMessage;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;

@RunWith(MockitoJUnitRunner.class)
public class OnrampImplTest {
  private final ObjectMapper mapper = new ObjectMapper();

  private Schema schema = SchemaBuilder.record("test_message").fields().requiredInt("id").endRecord();
  private SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);

  private @Mock Road road;
  private @Mock OnrampSender sender;
  private @Mock Random random;

  private OnMessage onMessage;
  private OnrampImpl underTest;

  @Before
  public void setUp() {
    when(road.getSchemas()).thenReturn(singletonMap(schemaVersion.getVersion(), schemaVersion));
    when(road.getPartitionPath()).thenReturn("$.udt.user.guid");
    when(sender.getPartitionCount(eq(road))).thenReturn(1);
    underTest = mock(
        OnrampImpl.class,
        withSettings().useConstructor(road, sender, random).defaultAnswer(CALLS_REAL_METHODS));
    onMessage = new OnMessage(null, mapper.createObjectNode().put("id", 123));
  }

  @Test
  public void sendEvent_calls_sender_correctly() throws Exception {
    ArgumentCaptor<InnerMessage> innerMsgCaptor = ArgumentCaptor.forClass(InnerMessage.class);
    Future<Boolean> future = CompletableFuture.completedFuture(true);
    when(sender.sendInnerMessage(eq(road), innerMsgCaptor.capture())).thenReturn(future);

    Future<Boolean> result = underTest.sendOnMessage(onMessage, Instant.now());

    assertThat(result, is(future));

    InnerMessage innerMessage = innerMsgCaptor.getValue();
    assertThat(innerMessage.getPartition(), is(0));
    assertNull(innerMessage.getKey());
    byte[] message = innerMessage.getMessage();
    assertThat(message[0], is((byte) 0x00));
    assertThat(Ints.fromBytes(message[1], message[2], message[3], message[4]), is(1));
  }

  @Test
  public void roadIsAvailable() {
    when(road.isEnabled()).thenReturn(true);

    assertThat(underTest.isAvailable(), is(true));
    assertThat(underTest.getRoad(), is(road));
  }

  @Test
  public void roadIsNotAvailable() {
    when(road.isEnabled()).thenReturn(false);

    assertThat(underTest.isAvailable(), is(false));
    assertThat(underTest.getRoad(), is(road));
  }
}
