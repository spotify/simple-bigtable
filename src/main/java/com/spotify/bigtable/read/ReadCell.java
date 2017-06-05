/*-
 * -\-\-
 * simple-bigtable
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

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
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

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

  public interface CellWithinColumnsRead extends CellWithinMulti<Column> {
    class ReadImpl extends CellWithinMultiImpl<Column> implements CellWithinColumnsRead {
      ReadImpl(final Internal<List<Column>> parent) {
        super(parent);
      }
    }
  }

  public interface CellWithinFamiliesRead extends CellWithinMulti<Family> {
    class ReadImpl extends CellWithinMultiImpl<Family> implements CellWithinFamiliesRead {
      ReadImpl(final Internal<List<Family>> parent) {
        super(parent);
      }
    }
  }

  public interface CellWithinRowsRead extends CellWithinMulti<Row> {
    class ReadImpl extends CellWithinMultiImpl<Row> implements CellWithinRowsRead {
      ReadImpl(final Internal<List<Row>> parent) {
        super(parent);
      }
    }
  }

  private interface CellWithinMulti<R> extends CellRead<List<R>> {
    class CellWithinMultiImpl<R> extends CellReadImpl<List<R>, List<R>> {

      private CellWithinMultiImpl(final Internal<List<R>> parent) {
        super(parent);
      }

      @Override
      protected Function<List<R>, List<R>> parentTypeToCurrentType() {
        return Function.identity();
      }
    }
  }

  private abstract static class CellReadImpl<P, R>
      extends AbstractBigtableRead<P, R> implements CellRead<R> {

    private CellReadImpl(final Internal<P> parent) {
      super(parent);
      final RowFilter.Builder cellFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(1);
      addRowFilter(cellFilter);
    }
  }
}

