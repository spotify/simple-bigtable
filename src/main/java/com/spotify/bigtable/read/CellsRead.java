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
import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.TimestampRange;
import com.google.bigtable.v1.ValueRange;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

public interface CellsRead extends BigtableRead<List<Cell>> {

  CellsRead latest();

  CellsRead limit(final int limit);

  CellsRead startTimestampMicros(final long startTimestampMicros);

  CellsRead endTimestampMicros(final long endTimestampMicros);

  CellsRead valueRegex(final ByteString valueRegex);

  CellsRead startValueInclusive(final ByteString startValueInclusive);

  CellsRead startValueExclusive(final ByteString startValueExclusive);

  CellsRead endValueInclusive(final ByteString endValueInclusive);

  CellsRead endValueExclusive(final ByteString endValueExclusive);

  class CellsReadImpl implements CellsRead, BigtableRead.Internal<List<Cell>> {

    private final BigtableRead.Internal<Optional<Column>> column;
    private final ReadRowsRequest.Builder readRequest;

    public CellsReadImpl(final BigtableRead.Internal<Optional<Column>> column) {
      super();
      this.column = column;
      this.readRequest = ReadRowsRequest.newBuilder(column.readRequest().build());
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      return readRequest;
    }

    @Override
    public BigtableDataClient getClient() {
      return column.getClient();
    }

    @Override
    public List<Cell> toDataType(List<Row> rows) {
      return column.toDataType(rows).map(Column::getCellsList).orElse(Lists.newArrayList());
    }


    @Override
    public ListenableFuture<List<Cell>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }

    @Override
    public CellsRead latest() {
      column.readRequest().clear().mergeFrom(readRequest().build());
      return new CellsReadImpl(column);
    }

    @Override
    public CellsRead limit(int limit) {
      final RowFilter limitFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(limit).build();
      readRequest.mergeFilter(limitFilter);
      return this;
    }

    @Override
    public CellsRead startTimestampMicros(long startTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setStartTimestampMicros(startTimestampMicros).build();
      final RowFilter limitFilter = RowFilter.newBuilder().setTimestampRangeFilter(tsRange).build();
      readRequest.mergeFilter(limitFilter);
      return this;
    }

    @Override
    public CellsRead endTimestampMicros(long endTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setEndTimestampMicros(endTimestampMicros).build();
      final RowFilter limitFilter = RowFilter.newBuilder().setTimestampRangeFilter(tsRange).build();
      readRequest.mergeFilter(limitFilter);
      return this;
    }

    @Override
    public CellsRead valueRegex(ByteString valueRegex) {
      readRequest.mergeFilter(RowFilter.newBuilder().setValueRegexFilter(valueRegex).build());
      return this;
    }

    @Override
    public CellsRead startValueInclusive(ByteString startValueInclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setStartValueInclusive(startValueInclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange).build());
      return this;
    }

    @Override
    public CellsRead startValueExclusive(ByteString startValueExclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setStartValueExclusive(startValueExclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange).build());
      return this;
    }

    @Override
    public CellsRead endValueInclusive(ByteString endValueInclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setEndValueInclusive(endValueInclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange).build());
      return this;
    }

    @Override
    public CellsRead endValueExclusive(ByteString endValueExclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setEndValueExclusive(endValueExclusive);
      readRequest.mergeFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange).build());
      return this;
    }
  }
}
