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

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;

import com.hotels.road.model.core.Road;
import com.hotels.road.onramp.api.OnrampSender;
import com.hotels.road.model.core.InnerMessage;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;

@Component
public class KafkaOnrampSender implements OnrampSender {
  private final OnrampMetrics metrics;
  private final Producer<byte[], byte[]> kafkaProducer;

  public KafkaOnrampSender(OnrampMetrics metrics, Producer<byte[], byte[]> kafkaProducer) {
    this.metrics = metrics;
    this.kafkaProducer = kafkaProducer;
  }

  @Override
  public Future<Boolean> sendInnerMessage(Road road, InnerMessage message) {
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        road.getTopicName(),
        message.getPartition(),
        message.getTimestampMs(),
        message.getKey(),
        message.getMessage());
    Future<RecordMetadata> future = kafkaProducer
        .send(record, (metadata, e) -> updateMetrics(road.getName(), metadata, e));
    return Futures.lazyTransform(future, metadata -> true);
  }

  @Override
  public int getPartitionCount(Road road) {
    return kafkaProducer.partitionsFor(road.getTopicName()).size();
  }

  private void updateMetrics(String roadName, RecordMetadata metadata, Exception e) {
    if (e == null) {
      metrics.markSuccessMetrics(roadName, metadata.serializedValueSize());
    } else {
      metrics.markFailureMetrics(roadName);
    }
  }

  @VisibleForTesting
  static Supplier<Path> pathSupplier(Road road) {
    return () -> Optional
        .ofNullable(road.getPartitionPath())
        .filter(p -> !p.isEmpty())
        .map(KeyPathParser::parse)
        .orElse(null);
  }
}
