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
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;

import java.util.List;
import java.util.Optional;

public interface ColumnsRead extends BigtableRead<List<Column>> {

  ColumnsRead familyName(final String familyName);

  ColumnsRead startQualifierClosed(final ByteString startQualifierClosed);

  ColumnsRead startQualifierOpen(final ByteString startQualifierOpen);

  ColumnsRead endQualifierCLosed(final ByteString endQualifierClosed);

  ColumnsRead endQualifierOpen(final ByteString endQualifierOpen);

  ColumnsRead latestCells();

  ColumnsRead startTimestampMicros(final long startTimestampMicros);

  ColumnsRead endTimestampMicros(final long startTimestampMicros);

  class ColumnsReadImpl extends AbstractBigtableRead<Optional<Row>, List<Column>> implements ColumnsRead {

    public ColumnsReadImpl(final BigtableRead.Internal<Optional<Row>> row) {
      super(row);
    }

    public ColumnsReadImpl(final Internal<Optional<Row>> row, final RowFilter.Builder rowFilter) {
      this(row);
      addRowFilter(rowFilter);
    }

    @Override
    public ColumnsRead familyName(final String familyName) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setFamilyName(familyName);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead startQualifierClosed(final ByteString startQualifierClosed) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierClosed(startQualifierClosed);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead startQualifierOpen(final ByteString startQualifierOpen) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierOpen(startQualifierOpen);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead endQualifierCLosed(final ByteString endQualifierClosed) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierClosed(endQualifierClosed);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead endQualifierOpen(final ByteString endQualifierOpen) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierOpen(endQualifierOpen);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead latestCells() {
      addRowFilter(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1));
      return this;
    }

    @Override
    public ColumnsRead startTimestampMicros(final long startTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setStartTimestampMicros(startTimestampMicros).build();
      addRowFilter(RowFilter.newBuilder().setTimestampRangeFilter(tsRange));
      return this;
    }

    @Override
    public ColumnsRead endTimestampMicros(final long endTimestampMicros) {
      final TimestampRange tsRange = TimestampRange.newBuilder().setEndTimestampMicros(endTimestampMicros).build();
      addRowFilter(RowFilter.newBuilder().setTimestampRangeFilter(tsRange));
      return this;
    }

    /**
     * Converts parent data type to return data type.
     *
     * Right now all columns must be in the same column family.
     * @param rowOptional Row Optional
     * @return List of columns in response
     */
    @Override
    protected List<Column> parentDataTypeToDataType(final Optional<Row> rowOptional) {
      return rowOptional.flatMap(row -> AbstractBigtableRead.headOption(row.getFamiliesList()))
              .map(Family::getColumnsList)
              .orElse(Lists.newArrayList());
    }
  }
}
