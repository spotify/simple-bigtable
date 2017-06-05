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

package com.spotify.bigtable.read;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.RowFilter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReadCell {

  interface CellRead<R> extends BigtableRead<R> { }

  public interface CellWithinCellsRead extends CellRead<Optional<Cell>> {
    class ReadImpl extends CellReadImpl<List<Cell>, Optional<Cell>> implements CellWithinCellsRead {

      ReadImpl(final Internal<List<Cell>> parent) {
        super(parent);
      }

      @Override
      protected Function<List<Cell>, Optional<Cell>> parentTypeToCurrentType() {
        return AbstractBigtableRead::headOption;
      }
    }
  }

  public interface CellWithinColumnsRead extends CellWithinMap {
    class ReadImpl extends CellWithinMap.ReadImpl implements CellWithinColumnsRead {
      ReadImpl(final Internal<Map<String, List<Cell>>> parent) {
        super(parent);
      }
    }
  }

  public interface CellWithinFamiliesRead extends CellWithinMap {
    class ReadImpl extends CellWithinMap.ReadImpl implements CellWithinFamiliesRead {
      ReadImpl(final Internal<Map<String, List<Cell>>> parent) {
        super(parent);
      }
    }
  }

  public interface CellWithinRowsRead extends CellWithinMap {
    class ReadImpl extends CellWithinMap.ReadImpl implements CellWithinRowsRead {
      ReadImpl(final Internal<Map<String, List<Cell>>> parent) {
        super(parent);
      }
    }
  }

  interface CellWithinMap extends CellRead<Map<String, Cell>> {
    class ReadImpl extends CellReadImpl<Map<String, List<Cell>>, Map<String, Cell>> implements CellWithinMap {

      private ReadImpl(final Internal<Map<String, List<Cell>>> parent) {
        super(parent);
      }

      @Override
      protected Function<Map<String, List<Cell>>, Map<String, Cell>> parentTypeToCurrentType() {
        return input -> input.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
      }
    }
  }

  public interface CellWithinColumnsAndFamiliesRead extends CellWithinTwoMaps {
    class ReadImpl extends CellWithinTwoMaps.ReadImpl implements CellWithinColumnsAndFamiliesRead {
      ReadImpl(final Internal<Map<String, Map<String, List<Cell>>>> parent) {
        super(parent);
      }
    }
  }

  public interface CellWithinColumnsAndRowsRead extends CellWithinTwoMaps {
    class ReadImpl extends CellWithinTwoMaps.ReadImpl implements CellWithinColumnsAndRowsRead {
      ReadImpl(final Internal<Map<String, Map<String, List<Cell>>>> parent) {
        super(parent);
      }
    }
  }

  public interface CellWithinFamiliesAndRowsRead extends CellWithinTwoMaps {
    class ReadImpl extends CellWithinTwoMaps.ReadImpl implements CellWithinFamiliesAndRowsRead {
      ReadImpl(final Internal<Map<String, Map<String, List<Cell>>>> parent) {
        super(parent);
      }
    }
  }

  interface CellWithinTwoMaps extends CellRead<Map<String, Map<String, Cell>>> {
    class ReadImpl extends CellReadImpl<Map<String, Map<String, List<Cell>>>, Map<String, Map<String, Cell>>>
        implements CellWithinTwoMaps {
      ReadImpl(final Internal<Map<String, Map<String, List<Cell>>>> parent) {
        super(parent);
      }

      @Override
      protected Function<Map<String, Map<String, List<Cell>>>, Map<String, Map<String, Cell>>> parentTypeToCurrentType() {
        return input -> input.entrySet().stream()
            .filter(row -> !fn().apply(row.getValue()).isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey,
                                      row -> fn().apply(row.getValue())));
      }

      protected Function<Map<String, List<Cell>>, Map<String, Cell>> fn() {
        return input -> input.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
      }
    }
  }

  public interface CellWithinColumnsWithinFamiliesWithinRowsRead extends CellRead<Map<String, Map<String, Map<String, Cell>>>> {
    class ReadImpl extends CellReadImpl<Map<String, Map<String, Map<String, List<Cell>>>>, Map<String, Map<String, Map<String, Cell>>>> implements CellWithinColumnsWithinFamiliesWithinRowsRead {
      ReadImpl(final Internal<Map<String, Map<String, Map<String, List<Cell>>>>> parent) {
        super(parent);
      }

      @Override
      protected Function<Map<String, Map<String, Map<String, List<Cell>>>>, Map<String, Map<String, Map<String, Cell>>>> parentTypeToCurrentType() {
        return null;
      }
    }
  }

  private abstract static class CellReadImpl<P, R> extends AbstractBigtableRead<P, R> implements CellRead<R> {

    private CellReadImpl(final Internal<P> parent) {
      super(parent);
      final RowFilter.Builder cellFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(1);
      addRowFilter(cellFilter);
    }
  }
}

