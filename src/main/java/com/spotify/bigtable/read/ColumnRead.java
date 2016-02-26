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
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableColumn;
import com.spotify.bigtable.Util;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

public interface ColumnRead extends BigtableRead<Optional<Column>> {

  CellRead latestCell();

  CellsRead cells();

  class ColumnReadImpl extends BigtableColumn implements ColumnRead, BigtableRead.Internal<Optional<Column>> {

    private final BigtableRead.Internal<Optional<Family>> family;

    public ColumnReadImpl(final BigtableRead.Internal<Optional<Family>> family, final String columnQualifier) {
      super(columnQualifier);
      this.family = family;
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      final RowFilter familyFilter = RowFilter.newBuilder()
              .setColumnQualifierRegexFilter(ByteString.copyFromUtf8(Util.toExactMatchRegex(columnQualifier))).build();
      return family.readRequest().mergeFilter(familyFilter);
    }

    @Override
    public BigtableDataClient getClient() {
      return family.getClient();
    }

    @Override
    public Optional<Column> toDataType(final List<Row> rows) {
      return family.toDataType(rows).flatMap(family -> Util.headOption(family.getColumnsList()));
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
