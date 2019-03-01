package com.hotels.road.loadingbay.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hotels.road.loadingbay.LoadingBayApp;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class HiveRoadTest {
  @Test
  public void nullType() throws Exception {
    HiveRoad road = HiveRoad.builder().build();

    ObjectMapper mapper = new LoadingBayApp().mapper();
    String roadString = mapper.writeValueAsString(road);
    road = mapper.readValue(roadString, HiveRoad.class);

    assertThat(road.getType(), is(nullValue()));
  }
}
