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
package com.hotels.road.offramp.version;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

public class OfframpVersionTest {

  @Test
  public void countValues() {
    assertThat(OfframpVersion.values().length, is(2));
  }

  @Test
  public void version2() {
    assertThat(OfframpVersion.fromString("2"), is(OfframpVersion.OFFRAMP_2));
  }

  @Test
  public void version3() {
    assertThat(OfframpVersion.fromString("3"), is(OfframpVersion.UNKNOWN));
  }

  @Test
  public void unknownVersion() {
    assertThat(OfframpVersion.fromString("1"), is(OfframpVersion.UNKNOWN));
  }

  @Test
  public void emptyString() {
    assertThat(OfframpVersion.fromString(""), is(OfframpVersion.UNKNOWN));
  }

  @Test
  public void nullString() {
    assertThat(OfframpVersion.fromString(null), is(OfframpVersion.UNKNOWN));
  }

  @Test
  public void string() {
    assertThat(OfframpVersion.OFFRAMP_2.toString(), is("2"));
    assertNull(OfframpVersion.UNKNOWN.toString());
  }

  @Test
  public void fromStringStrict() {
    assertThat(OfframpVersion.fromStringStrict("2"), is(OfframpVersion.OFFRAMP_2));
    try {
      OfframpVersion.fromStringStrict("3");
      fail("Should throw exception when creating Enum with wrong string using fromStringStrict");
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unknown version \"3\" was used"));
    }
  }

  @Test
  public void toApiVersion() {
    assertThat(OfframpVersion.OFFRAMP_2.toApiVersion(), is("v2"));
    assertNull(OfframpVersion.UNKNOWN.toApiVersion());
  }
}
