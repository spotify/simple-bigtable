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

import com.google.api.client.util.Lists;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.futures.FuturesExtra;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface ColumnsRead extends BigtableRead<List<Column>> {

  public ColumnsRead familyName(final String familyName);

  public ColumnsRead startQualifierInclusive(final ByteString startQualifierInclusive);

  public ColumnsRead startQualifierExclusive(final ByteString startQualifierExclusive);

  public ColumnsRead endQualifierInclusive(final ByteString endQualifierInclusive);

  public ColumnsRead endQualifierExclusive(final ByteString endQualifierExclusive);

  class ColumnsReadImpl implements ColumnsRead, BigtableRead.Internal<List<Column>> {

    private final BigtableRead.Internal<Optional<Row>> row;
    private final ReadRowsRequest.Builder readRequest;

    public ColumnsReadImpl(final BigtableRead.Internal<Optional<Row>> row) {
      this.row = row;
      this.readRequest = ReadRowsRequest.newBuilder(row.readRequest().build());
    }

    public ColumnsReadImpl(final BigtableRead.Internal<Optional<Row>> row, final ReadRowsRequest.Builder readRequest) {
      this.row = row;
      this.readRequest = readRequest;
    }

    @Override
    public ColumnsRead familyName(String familyName) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setFamilyName(familyName);
      readRequest.mergeFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange).build());
      return this;
    }

    @Override
    public ColumnsRead startQualifierInclusive(ByteString startQualifierInclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierInclusive(startQualifierInclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange).build());
      return this;
    }

    @Override
    public ColumnsRead startQualifierExclusive(ByteString startQualifierExclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierExclusive(startQualifierExclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange).build());
      return this;
    }

    @Override
    public ColumnsRead endQualifierInclusive(ByteString endQualifierInclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierInclusive(endQualifierInclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange).build());
      return this;
    }

    @Override
    public ColumnsRead endQualifierExclusive(ByteString endQualifierExclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierExclusive(endQualifierExclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange).build());
      return this;
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      return readRequest;
    }

    @Override
    public BigtableDataClient getClient() {
      return row.getClient();
    }

    @Override
    public List<Column> toDataType(List<Row> rows) {
      return row.toDataType(rows).map(row ->
                      row.getFamiliesList().stream()
                              .map(Family::getColumnsList)
                              .flatMap(Collection::stream)
                              .collect(Collectors.toList())
      ).orElse(Lists.newArrayList());
    }

    @Override
    public ListenableFuture<List<Column>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }
  }
}
