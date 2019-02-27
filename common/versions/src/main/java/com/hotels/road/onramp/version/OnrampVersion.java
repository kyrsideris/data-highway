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

import static lombok.AccessLevel.PRIVATE;

import com.hotels.road.api.version.ApiVersion;
import com.hotels.road.api.version.EnumVersion;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PRIVATE)
public enum OnrampVersion implements ApiVersion {
  ONRAMP_1("1"),
  ONRAMP_2("2"),
  UNKNOWN(null);

  private final String versionString;

  private static final EnumVersion<OnrampVersion> versioner =
      new EnumVersion<>(values(), UNKNOWN);

  public String getVersion() {
    return versionString;
  }

  public static OnrampVersion fromString(String versionString) {
    return versioner.fromString(versionString);
  }

  public static OnrampVersion fromStringStrict(String versionString) throws IllegalArgumentException {
    return versioner.fromStringStrict(versionString);
  }

  public String toString() {
    return versioner.toString(this);
  }

  public String toApiVersion() {
    return versioner.toApiVersion(this);
  }
}
