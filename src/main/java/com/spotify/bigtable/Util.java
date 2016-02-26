/*
 *
 *  * Copyright 2016 Spotify AB.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.spotify.bigtable;

import java.util.List;
import java.util.Optional;

public class Util {

  public static <T> Optional<T> headOption(final List<T> list) {
    if (list.size() > 1) {
      final String simpleName = list.get(0).getClass().getSimpleName();
      final String message = String.format("Multiple entities of type %s matched when only 1 expected", simpleName);
      throw new RuntimeException(message);
    }
    return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
  }

  public static String toExactMatchRegex(final String input) {
    // TODO: Need to escape all special chars in input
    return "/^" + input + "$/";
  }
  public static String toExactMatchAnyRegex(final List<String> inputs) {
    // TODO: Need to escape all special chars in inputs
    return toExactMatchRegex("(" + String.join("|", inputs) + ")");
  }
}
