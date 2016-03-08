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
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.TimestampRange;
import com.google.bigtable.v1.ValueRange;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

public interface CellsRead extends BigtableRead<List<Cell>> {

  CellRead latest();

  CellsRead limit(final int limit);

  CellsRead startTimestampMicros(final long startTimestampMicros);

  CellsRead endTimestampMicros(final long endTimestampMicros);

  CellsRead valueRegex(final ByteString valueRegex);

  CellsRead startValueInclusive(final ByteString startValueInclusive);

  CellsRead startValueExclusive(final ByteString startValueExclusive);

  CellsRead endValueInclusive(final ByteString endValueInclusive);

  CellsRead endValueExclusive(final ByteString endValueExclusive);

  class CellsReadImpl extends AbstractBigtableRead<Optional<Column>, List<Cell>> implements CellsRead {

    public CellsReadImpl(final BigtableRead.Internal<Optional<Column>> column) {
      super(column);
    }

    @Override
    protected List<Cell> parentDataTypeToDataType(final Optional<Column> column) {
      return column.map(Column::getCellsList).orElse(Lists.newArrayList());
    }

    @Override
    public ListenableFuture<List<Cell>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }

    @Override
    public CellRead latest() {
      // In order to allow the parent read to be reused we do not want to add the filters to the parents readRequest
      // Therefore we need to make sure the parent is unaltered. We probably should make a deep copy (hard to do
      // with an interface) but this hacky solution works for now
      final RowFilter oldFilter = parentRead.readRequest().getFilter();

      parentRead.readRequest().setFilter(readRequest().getFilter());
      final CellRead.CellReadImpl cellRead = new CellRead.CellReadImpl(parentRead);

      parentRead.readRequest().setFilter(oldFilter);
      return cellRead;
    }

    @Override
    public CellsRead limit(int limit) {
      final RowFilter.Builder limitFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(limit);
      addRowFilter(limitFilter);
      return this;
    }

    @Override
    public CellsRead startTimestampMicros(long startTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setStartTimestampMicros(startTimestampMicros).build();
      addRowFilter(RowFilter.newBuilder().setTimestampRangeFilter(tsRange));
      return this;
    }

    @Override
    public CellsRead endTimestampMicros(long endTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setEndTimestampMicros(endTimestampMicros).build();
      addRowFilter(RowFilter.newBuilder().setTimestampRangeFilter(tsRange));
      return this;
    }

    @Override
    public CellsRead valueRegex(ByteString valueRegex) {
      addRowFilter(RowFilter.newBuilder().setValueRegexFilter(valueRegex));
      return this;
    }

    @Override
    public CellsRead startValueInclusive(ByteString startValueInclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setStartValueInclusive(startValueInclusive);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }

    @Override
    public CellsRead startValueExclusive(ByteString startValueExclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setStartValueExclusive(startValueExclusive);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }

    @Override
    public CellsRead endValueInclusive(ByteString endValueInclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setEndValueInclusive(endValueInclusive);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }

    @Override
    public CellsRead endValueExclusive(ByteString endValueExclusive) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setEndValueExclusive(endValueExclusive);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }
  }
}
