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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.Ints;

import com.hotels.road.model.core.InnerMessage;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.rest.model.RoadType;

@RunWith(MockitoJUnitRunner.class)
public class OnrampImplTest {
  private final ObjectMapper mapper = new ObjectMapper();

  private Schema schema = SchemaBuilder.record("test_message").fields().requiredInt("id").endRecord();
  private SchemaVersion schemaVersion = new SchemaVersion(schema, 1, false);

  private @Mock Road roadNormal;
  private @Mock Road roadCompact;
  private @Mock Road roadNormalWoParPath;
  private @Mock Road roadNormalNullParPath;
  private @Mock OnrampSender sender;
  private @Mock Random random;
  private final int mockeRandomInt = 11111;

  private OnrampImpl underTestNormal;
  private OnrampImpl underTestCompact;
  private OnrampImpl underTestNormalWoParPath;
  private OnrampImpl underTestNormalNullParPath;

  private OnMessage msgNullKey;
  private OnMessage msgNullMessage;
  private OnMessage msgAllNull;
  private OnMessage msgAllNotNull;
  private OnMessage msgNullKeyWithWrongSchema;

  @Before
  public void setUp() {
    when(random.nextInt(anyInt())).thenReturn(mockeRandomInt);

    // normal road configuration
    when(roadNormal.getType()).thenReturn(RoadType.NORMAL);
    when(roadNormal.getSchemas()).thenReturn(singletonMap(schemaVersion.getVersion(), schemaVersion));
    when(roadNormal.getPartitionPath()).thenReturn("$.id");
    when(sender.getPartitionCount(eq(roadNormal))).thenReturn(36);
    underTestNormal = mockOnrampImplwithRoad(roadNormal);

    // compacted road configuration
    when(roadCompact.getType()).thenReturn(RoadType.COMPACT);
    when(roadCompact.getSchemas()).thenReturn(singletonMap(schemaVersion.getVersion(), schemaVersion));
    when(roadCompact.getPartitionPath()).thenReturn("$.id");
    when(sender.getPartitionCount(eq(roadCompact))).thenReturn(25);
    underTestCompact = mockOnrampImplwithRoad(roadCompact);

    // normal road configuration with partition path that is not correct
    when(roadNormalWoParPath.getType()).thenReturn(RoadType.NORMAL);
    when(roadNormalWoParPath.getSchemas()).thenReturn(singletonMap(schemaVersion.getVersion(), schemaVersion));
    when(roadNormalWoParPath.getPartitionPath()).thenReturn("$.idx");
    when(sender.getPartitionCount(eq(roadNormalWoParPath))).thenReturn(16);
    underTestNormalWoParPath = mockOnrampImplwithRoad(roadNormalWoParPath);

    when(roadNormalNullParPath.getType()).thenReturn(RoadType.NORMAL);
    when(roadNormalNullParPath.getSchemas()).thenReturn(singletonMap(schemaVersion.getVersion(), schemaVersion));
    when(roadNormalNullParPath.getPartitionPath()).thenReturn(null);

    when(sender.getPartitionCount(eq(roadNormalNullParPath))).thenReturn(8);
    underTestNormalNullParPath = mockOnrampImplwithRoad(roadNormalNullParPath);

    ObjectNode valueOnPartition = mapper.createObjectNode().put("id", 123);
    msgNullKey     = new OnMessage(null, valueOnPartition);
    msgNullMessage = new OnMessage("my key", null);
    msgAllNull     = new OnMessage(null, null);
    msgAllNotNull  = new OnMessage("my key", valueOnPartition);

    ObjectNode valueWithWrongSchema = mapper.createObjectNode().put("idy", 123);
    msgNullKeyWithWrongSchema = new OnMessage(null, valueWithWrongSchema);
  }

  @Test
  public void sendOnMessage_calls_sender_correctly_onNormalRoad() throws Exception {
    ArgumentCaptor<InnerMessage> innerMsgCaptor = ArgumentCaptor.forClass(InnerMessage.class);
    Future<Boolean> future = CompletableFuture.completedFuture(true);
    when(sender.sendInnerMessage(eq(roadNormal), innerMsgCaptor.capture())).thenReturn(future);
    when(sender.sendInnerMessage(eq(roadNormalWoParPath), innerMsgCaptor.capture())).thenReturn(future);
    when(sender.sendInnerMessage(eq(roadNormalNullParPath), innerMsgCaptor.capture())).thenReturn(future);

    Instant time = Instant.now();

    assertThat(underTestNormal.sendOnMessage(msgNullKey, time),is(future));

    InnerMessage innerMessage = innerMsgCaptor.getValue();
    assertThat(innerMessage.getPartition(), is(1));
    assertThat(innerMessage.getTimestampMs(), is(time.toEpochMilli()));
    assertNull(innerMessage.getKey());
    byte[] message = innerMessage.getMessage();
    assertThat(message[0], is((byte) 0x00));
    assertThat(Ints.fromBytes(message[1], message[2], message[3], message[4]), is(1));


    assertThat(underTestNormal.sendOnMessage(msgAllNotNull, time), is(future));

    InnerMessage msgInnerAllNotNull = innerMsgCaptor.getValue();
    assertThat(msgInnerAllNotNull.getPartition(), is(31));
    assertThat(msgInnerAllNotNull.getKey(), is("my key".getBytes(UTF_8)));


    mockRandomness(sender.getPartitionCount(roadNormalWoParPath));

    assertThat(underTestNormalWoParPath.sendOnMessage(msgNullKey, time), is(future));
    assertThat(
        innerMsgCaptor.getValue().getPartition(),
        is(expectedRandomness(sender.getPartitionCount(roadNormalWoParPath))));


    mockRandomness(sender.getPartitionCount(roadNormalNullParPath));

    assertThat(underTestNormalNullParPath.sendOnMessage(msgNullKey, time), is(future));
    assertThat(
        innerMsgCaptor.getValue().getPartition(),
        is(expectedRandomness(sender.getPartitionCount(roadNormalNullParPath))));
  }

  @Test
  public void sendOnMessage_failures_correctly_onNormalRoad() throws Exception {
    Instant time = Instant.now();

    assertFailedFuture(
        underTestNormal.sendOnMessage(msgNullMessage, time),
        "com.hotels.road.exception.InvalidEventException: "
            + "The event failed validation. Normal road messages must contain a message",
        "Exception was not raised for null message on OnMessage!"
    );

    assertFailedFuture(
        underTestNormal.sendOnMessage(msgAllNull, time),
        "com.hotels.road.exception.InvalidEventException: "
            + "The event failed validation. Normal road messages must contain a message",
        "Exception was not raised for null key and message on OnMessage!"
    );

    assertFailedFuture(
        underTestNormal.sendOnMessage(msgNullKeyWithWrongSchema, time),
        "com.hotels.road.exception.InvalidEventException: "
            + "The event failed validation. Field 'id': cannot make '\"int\"' value: 'null'",
        "Exception was not raised for message not conforming to schema!"
    );

  }

  @Test
  public void sendOnMessage_calls_sender_correctly_onCompactRoad() throws Exception {
    ArgumentCaptor<InnerMessage> innerMsgCaptor = ArgumentCaptor.forClass(InnerMessage.class);
    Future<Boolean> future = CompletableFuture.completedFuture(true);
    when(sender.sendInnerMessage(eq(roadCompact), innerMsgCaptor.capture())).thenReturn(future);

    Instant time = Instant.now();

    assertThat(
        underTestCompact.sendOnMessage(msgNullMessage, time),
        is(future));

    InnerMessage msgInnerNullMessage = innerMsgCaptor.getValue();
    assertThat(msgInnerNullMessage.getPartition(), is(18));
    assertThat(msgInnerNullMessage.getTimestampMs(), is(time.toEpochMilli()));
    assertThat(msgInnerNullMessage.getKey(), is("my key".getBytes(UTF_8)));
    assertNull(msgInnerNullMessage.getMessage());

    assertThat(
        underTestCompact.sendOnMessage(msgAllNotNull, time),
        is(future));

    InnerMessage msgInnerAllNotNull = innerMsgCaptor.getValue();
    assertThat(msgInnerAllNotNull.getPartition(), is(18));
    assertThat(msgInnerAllNotNull.getTimestampMs(), is(time.toEpochMilli()));
    assertThat(msgInnerAllNotNull.getKey(), is("my key".getBytes(UTF_8)));
    byte[] message = msgInnerAllNotNull.getMessage();
    assertThat(message[0], is((byte) 0x00));
    assertThat(Ints.fromBytes(message[1], message[2], message[3], message[4]), is(1));
  }

  @Test
  public void sendOnMessage_failures_correctly_onCompactRoad() throws Exception {
    Instant time = Instant.now();

    assertFailedFuture(
        underTestCompact.sendOnMessage(msgNullKey, time),
        "com.hotels.road.exception.InvalidEventException: "
            + "The event failed validation. Compact road messages must specify a key",
        "Exception was not raised for null message on OnMessage!"
    );

    assertFailedFuture(
        underTestCompact.sendOnMessage(msgAllNull, time),
        "com.hotels.road.exception.InvalidEventException: "
            + "The event failed validation. Compact road messages must specify a key",
        "Exception was not raised for null key and message on OnMessage!"
    );
  }

  @Test
  public void roadIsAvailable() {
    when(roadNormal.isEnabled()).thenReturn(true);

    assertThat(underTestNormal.isAvailable(), is(true));
    assertThat(underTestNormal.getRoad(), is(roadNormal));
  }

  @Test
  public void roadIsNotAvailable() {
    when(roadNormal.isEnabled()).thenReturn(false);

    assertThat(underTestNormal.isAvailable(), is(false));
    assertThat(underTestNormal.getRoad(), is(roadNormal));
  }


  void assertFailedFuture(Future<Boolean> future, String expected, String clue) {
    try {
      future.get();
      fail(clue);
    } catch (Exception e) {
      assertThat(e.getMessage(), is(expected));
    }
  }

  OnrampImpl mockOnrampImplwithRoad(Road road) {
    return mock(
        OnrampImpl.class,
        withSettings().useConstructor(road, sender, random).defaultAnswer(CALLS_REAL_METHODS));
  }

  void mockRandomness(int partitions) {
    when(random.nextInt(anyInt())).thenReturn(expectedRandomness(partitions));
  }

  int expectedRandomness(int partitions) {
    return mockeRandomInt % partitions;
  }
}
