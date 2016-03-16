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

import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Optional;

public interface FamilyRead extends BigtableRead<Optional<Family>> {

  ColumnRead columnQualifier(final ByteString columnQualifierBytes);

  ColumnRead columnQualifier(final String columnQualifier);

  ColumnsRead columnQualifierRegex(final ByteString columnQualifierRegexBytes);

  ColumnsRead columnQualifierRegex(final String columnQualifierRegex);

  ColumnsRead columnsQualifiers(final List<String> columnQualifiers);

  class FamilyReadImpl extends AbstractBigtableRead<Optional<Row>, Optional<Family>> implements FamilyRead {

    public FamilyReadImpl(final BigtableRead.Internal<Optional<Row>> row, final ByteString columnFamilyBytes) {
      super(row);

      final RowFilter.Builder familyFilter = RowFilter.newBuilder()
              .setFamilyNameRegexFilterBytes(columnFamilyBytes);
      addRowFilter(familyFilter);
    }

    public FamilyReadImpl(final BigtableRead.Internal<Optional<Row>> row, final String columnFamily) {
      this(row, ByteString.copyFromUtf8(toExactMatchRegex(columnFamily)));
    }

    @Override
    protected Optional<Family> parentDataTypeToDataType(final Optional<Row> row) {
      return row.flatMap(r -> AbstractBigtableRead.headOption(r.getFamiliesList()));
    }

    @Override
    public ColumnRead columnQualifier(final ByteString columnQualifierBytes) {
      return new ColumnRead.ColumnReadImpl(this, columnQualifierBytes);
    }

    @Override
    public ColumnRead columnQualifier(final String columnQualifier) {
      return new ColumnRead.ColumnReadImpl(this, columnQualifier);
    }

    @Override
    public ColumnsRead columnQualifierRegex(final ByteString columnQualifierRegexBytes) {
      final RowFilter.Builder columnFilter = RowFilter.newBuilder()
              .setColumnQualifierRegexFilter(columnQualifierRegexBytes);

      // In order to allow the parent read to be reused we do not want to add the filters to the parents readRequest
      // Therefore we need to make sure the parent is unaltered. We probably should make a deep copy (hard to do
      // with an interface) but this hacky solution works for now
      final RowFilter oldFilter = parentRead.readRequest().getFilter();

      parentRead.readRequest().setFilter(readRequest().getFilter());
      final ColumnsRead.ColumnsReadImpl columnsRead = new ColumnsRead.ColumnsReadImpl(parentRead, columnFilter);

      parentRead.readRequest().setFilter(oldFilter);
      return columnsRead;
    }

    @Override
    public ColumnsRead columnQualifierRegex(final String columnQualifierRegex) {
      return columnQualifierRegex(ByteString.copyFromUtf8(columnQualifierRegex));
    }

    @Override
    public ColumnsRead columnsQualifiers(final List<String> columnQualifiers) {
      return columnQualifierRegex(toExactMatchAnyRegex(columnQualifiers));
    }
  }
}

