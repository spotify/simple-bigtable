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

import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowRange;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.protobuf.ByteString;

import java.util.List;

public interface RowsRead extends BigtableRead<List<Row>> {

  RowsRead limit(final long limit);

  RowsRead allowRowInterleaving(final boolean allowRowInterleaving);

  RowsRead startKey(final ByteString startKey);

  RowsRead endKey(final ByteString endKey);

  ResultScanner<Row> execute();

  class RowsReadImpl extends AbstractBigtableRead<List<Row>, List<Row>> implements RowsRead {

    public RowsReadImpl(final TableRead.TableReadImpl tableRead) {
      super(tableRead);
    }

    @Override
    protected List<Row> parentDataTypeToDataType(final List<Row> rows) {
      return rows;
    }

    @Override
    public RowsRead limit(final long limit) {
      readRequest.setNumRowsLimit(limit);
      return this;
    }

    @Override
    public RowsRead allowRowInterleaving(final boolean allowRowInterleaving) {
      readRequest.setAllowRowInterleaving(allowRowInterleaving);
      return this;
    }

    @Override
    public RowsRead startKey(final ByteString startKey) {
      final RowRange.Builder rowRange = RowRange.newBuilder(readRequest.getRowRange()).setStartKey(startKey);
      readRequest.setRowRange(rowRange);
      return this;
    }

    @Override
    public RowsRead endKey(final ByteString endKey) {
      final RowRange.Builder rowRange = RowRange.newBuilder(readRequest.getRowRange()).setEndKey(endKey);
      readRequest.setRowRange(rowRange);
      return this;
    }

    @Override
    public ResultScanner<Row> execute() {
      return getClient().readRows(readRequest().build());
    }
  }
}
