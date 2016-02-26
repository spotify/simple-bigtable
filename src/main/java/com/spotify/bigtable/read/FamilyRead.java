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
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableFamily;
import com.spotify.bigtable.Util;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

public interface FamilyRead extends BigtableRead<Optional<Family>> {

  ColumnRead columnQualifier(final String columnQualifier);

  ColumnsRead columnQualifierRegex(final String columnQualifierRegex);

  ColumnsRead columnsQualifiers(final List<String> columnQualifiers);

  class FamilyReadImpl extends BigtableFamily implements FamilyRead, BigtableRead.Internal<Optional<Family>> {

    private final BigtableRead.Internal<Optional<Row>> row;

    public FamilyReadImpl(final BigtableRead.Internal<Optional<Row>> row, final String columnFamily) {
      super(columnFamily);
      this.row = row;
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      final RowFilter familyFilter = RowFilter.newBuilder()
              .setFamilyNameRegexFilter(Util.toExactMatchRegex(columnFamily)).build();
      return row.readRequest().mergeFilter(familyFilter);
    }

    @Override
    public BigtableDataClient getClient() {
      return row.getClient();
    }

    @Override
    public Optional<Family> toDataType(List<Row> rows) {
      return  row.toDataType(rows).flatMap(row -> Util.headOption(row.getFamiliesList()));
    }

    @Override
    public ListenableFuture<Optional<Family>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }

    @Override
    public ColumnRead columnQualifier(String columnQualifier) {
      return new ColumnRead.ColumnReadImpl(this, columnQualifier);
    }

    @Override
    public ColumnsRead columnQualifierRegex(String columnQualifierRegex) {
      final ReadRowsRequest.Builder readRequest = ReadRowsRequest.newBuilder(readRequest().build());
      final ByteString columnRegexBytes = ByteString.copyFromUtf8(columnQualifierRegex);
      final RowFilter.Builder columnFilter = RowFilter.newBuilder().setColumnQualifierRegexFilter(columnRegexBytes);
      readRequest.mergeFilter(columnFilter.build());
      return new ColumnsRead.ColumnsReadImpl(row, readRequest);
    }

    @Override
    public ColumnsRead columnsQualifiers(List<String> columnQualifiers) {
      return columnQualifierRegex(Util.toExactMatchAnyRegex(columnQualifiers));
    }
  }
}

