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

import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface RowRead extends BigtableRead<Optional<Row>> {

  FamilyRead family(final String columnFamily);

  FamiliesRead familyRegex(final String columnFamilyRegex);

  FamiliesRead families(final List<String> families);

  ColumnRead column(final String column);

  ColumnsRead columns();

  ColumnsRead columns(final List<String> columns);

  class RowReadImpl extends AbstractBigtableRead<List<Row>, Optional<Row>> implements RowRead {

    public RowReadImpl(final TableRead.TableReadImpl tableRead, final String row) {
      super(tableRead);
      readRequest.setRows(RowSet.newBuilder().addRowKeys(ByteString.copyFromUtf8(row)));
      readRequest.setRowsLimit(1);
    }

    @Override
    protected Optional<Row> parentDataTypeToDataType(final List<Row> rows) {
      return AbstractBigtableRead.headOption(rows);
    }

    @Override
    public FamilyRead family(final String columnFamily) {
      return new FamilyRead.FamilyReadImpl(this, columnFamily);
    }

    @Override
    public FamiliesRead familyRegex(final String columnFamilyRegex) {
      final RowFilter.Builder familyFilter = RowFilter.newBuilder().setFamilyNameRegexFilter(columnFamilyRegex);
      return new FamiliesRead.FamiliesReadImpl(this, familyFilter);
    }

    @Override
    public FamiliesRead families(final List<String> families) {
      return familyRegex(toExactMatchAnyRegex(families));
    }

    @Override
    public ColumnRead column(final String column) {
      final List<String> parts = Arrays.asList(column.split(":", 2));
      return family(parts.get(0)).columnQualifier(parts.get(1));
    }

    @Override
    public ColumnsRead columns() {
      return new ColumnsRead.ColumnsReadImpl(this);
    }

    /**
     * Get all columns from list of columnFam:columnQualifier strings
     *
     * NOTE: Currently only works with all same column family
     * @param columns list of columns
     * @return ColumnsRead
     */
    @Override
    public ColumnsRead columns(final List<String> columns) {
      final String columnFam = columns.get(0).split(":", 2)[0];
      final boolean allSameColumnFam = columns.stream()
              .allMatch(column -> columnFam.equals(column.split(":", 2)[0]));

      if (!allSameColumnFam) {
        throw new RuntimeException("Only same column family supported for operation");
      }
      final List<String> qualifiers = columns.stream()
              .map(column -> column.split(":", 2)[1])
              .collect(Collectors.toList());
      return family(columnFam).columnsQualifiers(qualifiers);
    }
  }
}
