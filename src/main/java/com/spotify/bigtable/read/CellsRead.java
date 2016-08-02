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
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.bigtable.v2.ValueRange;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Optional;

public interface CellsRead extends BigtableRead<List<Cell>> {

  CellRead latest();

  CellsRead limit(final int limit);

  CellsRead startTimestampMicros(final long startTimestampMicros);

  CellsRead endTimestampMicros(final long endTimestampMicros);

  CellsRead valueRegex(final ByteString valueRegex);

  CellsRead startValueClosed(final ByteString startValueInclusive);

  CellsRead startValueOpen(final ByteString startValueExclusive);

  CellsRead endValueClosed(final ByteString endValueInclusive);

  CellsRead endValueOpen(final ByteString endValueExclusive);

  class CellsReadImpl extends AbstractBigtableRead<Optional<Column>, List<Cell>> implements CellsRead {

    public CellsReadImpl(final BigtableRead.Internal<Optional<Column>> column) {
      super(column);
    }

    @Override
    protected List<Cell> parentDataTypeToDataType(final Optional<Column> column) {
      return column.map(Column::getCellsList).orElse(Lists.newArrayList());
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
    public CellsRead limit(final int limit) {
      final RowFilter.Builder limitFilter = RowFilter.newBuilder().setCellsPerColumnLimitFilter(limit);
      addRowFilter(limitFilter);
      return this;
    }

    @Override
    public CellsRead startTimestampMicros(final long startTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setStartTimestampMicros(startTimestampMicros).build();
      addRowFilter(RowFilter.newBuilder().setTimestampRangeFilter(tsRange));
      return this;
    }

    @Override
    public CellsRead endTimestampMicros(final long endTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setEndTimestampMicros(endTimestampMicros).build();
      addRowFilter(RowFilter.newBuilder().setTimestampRangeFilter(tsRange));
      return this;
    }

    @Override
    public CellsRead valueRegex(final ByteString valueRegex) {
      addRowFilter(RowFilter.newBuilder().setValueRegexFilter(valueRegex));
      return this;
    }

    @Override
    public CellsRead startValueClosed(final ByteString startValueClosed) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setStartValueClosed(startValueClosed);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }

    @Override
    public CellsRead startValueOpen(final ByteString startValueOpen) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setStartValueOpen(startValueOpen);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }

    @Override
    public CellsRead endValueClosed(final ByteString endValueClosed) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setEndValueClosed(endValueClosed);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }

    @Override
    public CellsRead endValueOpen(final ByteString endValueOpen) {
      final ValueRange.Builder valueRange = ValueRange.newBuilder().setEndValueOpen(endValueOpen);
      addRowFilter(RowFilter.newBuilder().setValueRangeFilter(valueRange));
      return this;
    }
  }
}
