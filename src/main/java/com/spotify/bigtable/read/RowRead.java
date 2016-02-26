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

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableRow;
import com.spotify.bigtable.Util;
import com.spotify.futures.FuturesExtra;

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

  class RowReadImpl extends BigtableRow implements RowRead, BigtableRead.Internal<Optional<Row>> {

    private final TableRead.TableReadImpl tableRead;

    public RowReadImpl(final TableRead.TableReadImpl tableRead, final String row) {
      super(tableRead, row);
      this.tableRead = tableRead;
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      return tableRead.readRequest().setRowKey(ByteString.copyFromUtf8(row)).setNumRowsLimit(1);
    }

    @Override
    public BigtableDataClient getClient() {
      return tableRead.getClient();
    }

    @Override
    public Optional<Row> toDataType(final List<Row> rows) {
      return Util.headOption(rows);
    }

    @Override
    public FamilyRead family(final String columnFamily) {
      return new FamilyRead.FamilyReadImpl(this, columnFamily);
    }

    @Override
    public FamiliesRead familyRegex(final String columnFamilyRegex) {
      final ReadRowsRequest.Builder readRequest = readRequest();
      final RowFilter.Builder familyFilter = RowFilter.newBuilder()
              .setFamilyNameRegexFilter(Util.toExactMatchRegex(columnFamilyRegex));
      readRequest.mergeFilter(familyFilter.build());
      return new FamiliesRead.FamiliesReadImpl(this, readRequest);
    }

    @Override
    public FamiliesRead families(List<String> families) {
      return familyRegex(Util.toExactMatchAnyRegex(families));
    }

    @Override
    public ColumnRead column(String column) {
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
    public ColumnsRead columns(List<String> columns) {
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

    @Override
    public ListenableFuture<Optional<Row>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }

  }
}
