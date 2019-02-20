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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.InnerMessage;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;

@RunWith(MockitoJUnitRunner.class)
public class KafkaOnrampSenderTest {

  private static final String ROAD_NAME = "test-onramp";
  private static final Schema SCHEMA = SchemaBuilder
      .builder()
      .record("r")
      .fields()
      .name("f")
      .type()
      .stringType()
      .noDefault()
      .endRecord();

  @Mock
  private Road road;
  @Mock
  private OnrampMetrics metrics;
  @Mock
  private Producer<byte[], byte[]> kafkaProducer;
  @Mock
  private Future<RecordMetadata> future;

  private final ObjectMapper mapper = new ObjectMapper();
  private KafkaOnrampSender underTest;

  @Before
  public void setUp() {
    when(road.getName()).thenReturn(ROAD_NAME);
    when(road.getTopicName()).thenReturn(ROAD_NAME);
    underTest = new KafkaOnrampSender(metrics, kafkaProducer);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendFails()
    throws InvalidEventException, InterruptedException, ExecutionException, JsonProcessingException, IOException {
    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(future);
    doThrow(new ExecutionException(new BufferExhaustedException("exhausted"))).when(future).get();

    Future<Boolean> result = underTest.sendInnerMessage(road, new InnerMessage(0, 1550482463, new byte[0], new byte[0]));

    try {
      result.get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(BufferExhaustedException.class));
      return;
    }
    fail("Expected ExecutionException");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendSucceeds()
    throws InvalidEventException, InterruptedException, ExecutionException, JsonProcessingException, IOException {
    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(future);

    Future<Boolean> result = underTest.sendInnerMessage(road, new InnerMessage(0, 1550482463, new byte[0], new byte[0]));

    assertThat(result.get(), is(true));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendEncodedEvent_UpdateMetrics_Success() throws Exception {
    RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, Long.valueOf(0), 0, 1);
    Exception exception = null;

    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      ((Callback) invocation.getArgument(1)).onCompletion(metadata, exception);
      return future;
    });

    underTest.sendInnerMessage(road, new InnerMessage(0, 1550482463, new byte[0], new byte[0]));

    verify(metrics).markSuccessMetrics(ROAD_NAME, 1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sendEncodedEvent_UpdateMetrics_Failure() throws Exception {
    RecordMetadata metadata = null;
    Exception exception = new Exception();

    when(kafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      ((Callback) invocation.getArgument(1)).onCompletion(metadata, exception);
      return future;
    });

    underTest.sendInnerMessage(road, new InnerMessage(0, 1550482463, new byte[0], new byte[0]));

    verify(metrics).markFailureMetrics(ROAD_NAME);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void sendMultiple() throws Exception {
    ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
    when(kafkaProducer.send(captor.capture(), any(Callback.class))).thenReturn(future);

    underTest.sendInnerMessage(road, new InnerMessage(0, 1550482463, new byte[0], new byte[] { 0x01, 0x02 }));
    underTest.sendInnerMessage(road, new InnerMessage(0, 1550482463, new byte[0], new byte[] { 0x03, 0x04 }));

    List<ProducerRecord> values = captor.getAllValues();

    assertThat(((ProducerRecord<byte[], byte[]>) values.get(0)).value()[0], is((byte)0x01));
    assertThat(((ProducerRecord<byte[], byte[]>) values.get(0)).value()[1], is((byte)0x02));
    assertThat(((ProducerRecord<byte[], byte[]>) values.get(1)).value()[0], is((byte)0x03));
    assertThat(((ProducerRecord<byte[], byte[]>) values.get(1)).value()[1], is((byte)0x04));
  }

  @Test
  public void pathSupplier() {
    when(road.getPartitionPath()).thenReturn("$.a");
    Supplier<Path> supplier = KafkaOnrampSender.pathSupplier(road);
    assertThat(supplier.get(), is(KeyPathParser.parse("$.a")));
  }

  @Test
  public void pathSupplierNull() {
    when(road.getPartitionPath()).thenReturn(null);
    Supplier<Path> supplier = KafkaOnrampSender.pathSupplier(road);
    assertThat(supplier.get(), is(nullValue()));
  }

  @Test
  public void pathSupplierEmpty() {
    when(road.getPartitionPath()).thenReturn("");
    Supplier<Path> supplier = KafkaOnrampSender.pathSupplier(road);
    assertThat(supplier.get(), is(nullValue()));
  }

}
