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
package com.hotels.road.api.version;

import java.util.HashMap;
import java.util.Map;

public class EnumVersion<E extends Enum<E> & ApiVersion>{

  private Map<String, E> lookup;
  private E unknown;

  public EnumVersion(E[] values, E unknown) {
    this.lookup = new HashMap<>();
    for (E e : values) {
      this.lookup.put(e.getVersion(), e);
    }
    this.unknown = unknown;

  }

  public E fromString(String versionString){
    return lookup.getOrDefault(versionString, unknown);
  }

  public E fromStringStrict(String versionString) throws IllegalArgumentException {
    E version = lookup.getOrDefault(versionString, unknown);
    if (version == unknown) {
      throw new IllegalArgumentException(String.format("Unknown version \"%s\" was used", versionString));
    }
    return version;
  }

  public String toString(E e) {
    if (e == null) {
      return null;
    }
    return e.getVersion();
  }

  public String toApiVersion(E e) {
    if (e == null || e.getVersion() == null || e.getVersion().equals(unknown)){
      return null;
    }
    return "v" + e.getVersion();
  }
}
