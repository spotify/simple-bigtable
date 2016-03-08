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
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Optional;

public interface ColumnsRead extends BigtableRead<List<Column>> {

  public ColumnsRead familyName(final String familyName);

  public ColumnsRead startQualifierInclusive(final ByteString startQualifierInclusive);

  public ColumnsRead startQualifierExclusive(final ByteString startQualifierExclusive);

  public ColumnsRead endQualifierInclusive(final ByteString endQualifierInclusive);

  public ColumnsRead endQualifierExclusive(final ByteString endQualifierExclusive);

  class ColumnsReadImpl extends AbstractBigtableRead<Optional<Row>, List<Column>> implements ColumnsRead {

    public ColumnsReadImpl(final BigtableRead.Internal<Optional<Row>> row) {
      super(row);
    }

    public ColumnsReadImpl(Internal<Optional<Row>> row, RowFilter.Builder rowFilter) {
      this(row);
      addRowFilter(rowFilter);
    }

    @Override
    public ColumnsRead familyName(String familyName) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setFamilyName(familyName);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead startQualifierInclusive(ByteString startQualifierInclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierInclusive(startQualifierInclusive);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead startQualifierExclusive(ByteString startQualifierExclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierExclusive(startQualifierExclusive);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead endQualifierInclusive(ByteString endQualifierInclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierInclusive(endQualifierInclusive);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return this;
    }

    @Override
    public ColumnsRead endQualifierExclusive(ByteString endQualifierExclusive) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierExclusive(endQualifierExclusive);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
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
    protected List<Column> parentDataTypeToDataType(Optional<Row> rowOptional) {
      return rowOptional.flatMap(row -> AbstractBigtableRead.headOption(row.getFamiliesList()))
              .map(Family::getColumnsList)
              .orElse(Lists.newArrayList());
    }
  }
}
