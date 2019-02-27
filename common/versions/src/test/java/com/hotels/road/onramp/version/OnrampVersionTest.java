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
package com.hotels.road.onramp.version;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

public class OnrampVersionTest {
  @Test
  public void countValues() {
    assertThat(OnrampVersion.values().length, is(3));
  }

  @Test
  public void version1() {
    assertThat(OnrampVersion.fromString("1"), is(OnrampVersion.ONRAMP_1));
  }

  @Test
  public void version2() {
    assertThat(OnrampVersion.fromString("2"), is(OnrampVersion.ONRAMP_2));
  }

  @Test
  public void version3() {
    assertThat(OnrampVersion.fromString("3"), is(OnrampVersion.UNKNOWN));
  }

  @Test
  public void unknownVersion() {
    assertThat(OnrampVersion.fromString("0"), is(OnrampVersion.UNKNOWN));
  }

  @Test
  public void emptyString() {
    assertThat(OnrampVersion.fromString(""), is(OnrampVersion.UNKNOWN));
  }

  @Test
  public void nullString() {
    assertThat(OnrampVersion.fromString(null), is(OnrampVersion.UNKNOWN));
  }

  @Test
  public void string() {
    assertThat(OnrampVersion.ONRAMP_1.toString(), is("1"));
    assertThat(OnrampVersion.ONRAMP_2.toString(), is("2"));
    assertNull(OnrampVersion.UNKNOWN.toString());
  }

  @Test
  public void fromStringStrict() {
    assertThat(OnrampVersion.fromStringStrict("1"), is(OnrampVersion.ONRAMP_1));
    assertThat(OnrampVersion.fromStringStrict("2"), is(OnrampVersion.ONRAMP_2));
    try {
      OnrampVersion.fromStringStrict("3");
      fail("Should throw exception when creating Enum with wrong string using fromStringStrict");
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unknown version \"3\" was used"));
    }
  }

  @Test
  public void toApiVersion() {
    assertThat(OnrampVersion.ONRAMP_1.toApiVersion(), is("v1"));
    assertThat(OnrampVersion.ONRAMP_2.toApiVersion(), is("v2"));
    assertNull(OnrampVersion.UNKNOWN.toApiVersion());
  }
}

