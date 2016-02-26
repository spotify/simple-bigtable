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

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.bigtable.Util;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

public interface CellRead extends BigtableRead<Optional<Cell>> {

  class CellReadImpl implements CellRead, BigtableRead.Internal<Optional<Cell>> {

    private final BigtableRead.Internal<Optional<Column>> column;

    public CellReadImpl(final BigtableRead.Internal<Optional<Column>> column) {
      super();
      this.column = column;
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      final RowFilter cellFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build();
      return column.readRequest().mergeFilter(cellFilter);
    }

    @Override
    public BigtableDataClient getClient() {
      return column.getClient();
    }

    @Override
    public Optional<Cell> toDataType(List<Row> rows) {
      return column.toDataType(rows).flatMap(column -> Util.headOption(column.getCellsList()));
    }


    @Override
    public ListenableFuture<Optional<Cell>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }
  }
}
