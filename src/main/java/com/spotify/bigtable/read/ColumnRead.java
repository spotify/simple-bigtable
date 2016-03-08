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

import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.RowFilter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.futures.FuturesExtra;

import java.util.Optional;

public interface ColumnRead extends BigtableRead<Optional<Column>> {

  CellRead latestCell();

  CellsRead cells();

  class ColumnReadImpl extends AbstractBigtableRead<Optional<Family>, Optional<Column>> implements ColumnRead {

    public ColumnReadImpl(final BigtableRead.Internal<Optional<Family>> family, final String columnQualifier) {
      super(family);

      final RowFilter.Builder qualifierFilter = RowFilter.newBuilder()
              .setColumnQualifierRegexFilter(ByteString.copyFromUtf8(toExactMatchRegex(columnQualifier)));
      addRowFilter(qualifierFilter);
    }

    @Override
    protected Optional<Column> parentDataTypeToDataType(Optional<Family> family) {
      return family.flatMap(f -> AbstractBigtableRead.headOption(f.getColumnsList()));
    }

    @Override
    public ListenableFuture<Optional<Column>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }

    @Override
    public CellRead latestCell() {
      return new CellRead.CellReadImpl(this);
    }

    @Override
    public CellsRead cells() {
      return new CellsRead.CellsReadImpl(this);
    }
  }

}
