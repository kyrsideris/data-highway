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
package com.hotels.road.onramp.controller;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.hotels.road.exception.InvalidEventException;
import com.hotels.road.exception.RoadUnavailableException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.onramp.api.OnMessage;
import com.hotels.road.onramp.api.Onramp;
import com.hotels.road.onramp.api.OnrampService;
import com.hotels.road.rest.model.StandardResponse;

import io.micrometer.core.instrument.MeterRegistry;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Api(tags = "onramp")
@RestController
@RequestMapping("/onramp")
@RequiredArgsConstructor
@Slf4j
public class OnrampController {

  private static final String MESSAGE_ACCEPTED = "Message accepted.";
  private final OnrampService service;
  private final MeterRegistry registry;
  private final Clock clock;
  private final ObjectMapper v2Mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

  @ApiOperation(value = "Sends a given array of messages to a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Messages have been sent successfully.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Bad Request.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class),
      @ApiResponse(code = 422, message = "Road not enabled.", response = StandardResponse.class) })
  @PreAuthorize("@onrampAuthorisation.isAuthorised(authentication,#roadName)")
  @PostMapping(path = "/v1/roads/{roadName}/messages")
  public List<StandardResponse> produce(@PathVariable String roadName, @RequestBody Iterable<ObjectNode> messages)
      throws UnknownRoadException, InterruptedException {
    return sendMessages(
        roadName,
        StreamSupport
            .stream(messages.spliterator(), false)
            .map(this::mapToOnmessageV1));
  }

  @ApiOperation(value = "Sends a given array of messages to a road")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Messages have been sent successfully.", response = StandardResponse.class),
      @ApiResponse(code = 400, message = "Bad Request.", response = StandardResponse.class),
      @ApiResponse(code = 404, message = "Road not found.", response = StandardResponse.class),
      @ApiResponse(code = 422, message = "Road not enabled.", response = StandardResponse.class) })
  @PreAuthorize("@onrampAuthorisation.isAuthorised(authentication,#roadName)")
  @PostMapping(path = "/v2/roads/{roadName}/messages")
  public List<StandardResponse> produceV2(@PathVariable String roadName, @RequestBody Iterable<ObjectNode> messages)
      throws UnknownRoadException, InterruptedException {
    return sendMessages(
        roadName,
        StreamSupport
            .stream(messages.spliterator(), false)
            .map(this::mapToOnmessageV2));
  }

  private Onramp getOnramp(String roadName) throws UnknownRoadException {
    Onramp onramp = service.getOnramp(roadName).orElseThrow(() -> new UnknownRoadException(roadName));
    if (!onramp.isAvailable()) {
      throw new RoadUnavailableException(String.format("Road '%s' is disabled, could not send events.", roadName));
    }
    return onramp;
  }

  private List<StandardResponse> sendMessages(String roadName, Stream<OnMessage> messages)
      throws ServiceException, UnknownRoadException {
    Onramp onramp = getOnramp(roadName);
    Instant time = clock.instant();
    return messages
        .map(m -> onramp.sendOnMessage(m, time))
        .map(this::translateFuture)
        .collect(Collectors.toList());
  }

  private StandardResponse translateFuture(Future<Boolean> future) throws ServiceException {
    try {
      future.get();
      return StandardResponse.successResponse(MESSAGE_ACCEPTED);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (!(cause instanceof InvalidEventException)) {
        log.warn("Problem sending event", e);
      }
      return StandardResponse.failureResponse(cause.getMessage());
    } catch (InterruptedException e) {
      throw new ServiceException(e);
    }
  }

  private OnMessage mapToOnmessageV1(ObjectNode obj) {
    return new OnMessage(null, obj);
  }


  private OnMessage mapToOnmessageV2(ObjectNode obj) throws IllegalArgumentException {
    try {
      return v2Mapper.treeToValue(obj, OnMessage.class);
    } catch (JsonProcessingException e) {
      log.warn("Failed to map ObjectNode to OnMessage");
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @ExceptionHandler
  @ResponseStatus(UNPROCESSABLE_ENTITY)
  public StandardResponse roadUnavailable(RoadUnavailableException e) {
    return StandardResponse.failureResponse(e.getMessage());
  }

  @ExceptionHandler
  @ResponseStatus(NOT_FOUND)
  public StandardResponse unknownRoadException(UnknownRoadException e) {
    log.warn(NOT_FOUND.getReasonPhrase(), e);
    registry.counter("onramp.road_not_found").increment();
    return StandardResponse.failureResponse(e.getMessage());
  }
}
