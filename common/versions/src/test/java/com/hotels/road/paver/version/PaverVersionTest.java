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
package com.hotels.road.paver.version;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

public class PaverVersionTest {

  @Test
  public void countValues() {
    assertThat(PaverVersion.values().length, is(2));
  }

  @Test
  public void version1() {
    assertThat(PaverVersion.fromString("1"), is(PaverVersion.PAVER_1));
  }

  @Test
  public void version2() {
    assertThat(PaverVersion.fromString("2"), is(PaverVersion.UNKNOWN));
  }

  @Test
  public void unknownVersion() {
    assertThat(PaverVersion.fromString("0"), is(PaverVersion.UNKNOWN));
  }

  @Test
  public void emptyString() {
    assertThat(PaverVersion.fromString(""), is(PaverVersion.UNKNOWN));
  }

  @Test
  public void nullString() {
    assertThat(PaverVersion.fromString(null), is(PaverVersion.UNKNOWN));
  }

  @Test
  public void string() {
    assertThat(PaverVersion.PAVER_1.toString(), is("1"));
    assertNull(PaverVersion.UNKNOWN.toString());
  }

  @Test
  public void fromStringStrict() {
    assertThat(PaverVersion.fromStringStrict("1"), is(PaverVersion.PAVER_1));
    try {
      PaverVersion.fromStringStrict("2");
      fail("Should throw exception when creating Enum with wrong string using fromStringStrict");
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Unknown version \"2\" was used"));
    }
  }

  @Test
  public void toApiVersion() {
    assertThat(PaverVersion.PAVER_1.toApiVersion(), is("v1"));
    assertNull(PaverVersion.UNKNOWN.toApiVersion());
  }
}
