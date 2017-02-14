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

import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RowsRead is an interface for reading a set of rows.
 */
public interface RowsRead extends BigtableRead<List<Row>> {

  RowsRead addKeys(final Collection<String> rowKeys);

  RowsRead limit(final long limit);

  RowsRead addRowRangeOpen(final ByteString startKeyOpen, final ByteString endKeyOpen);

  RowsRead addRowRangeClosed(final ByteString startKeyClosed, final ByteString endKeyClosed);

  RowRead withinRow();

  ResultScanner<Row> execute();

  class RowsReadImpl extends AbstractBigtableRead<List<Row>, List<Row>> implements RowsRead {

    public RowsReadImpl(final TableRead.TableReadImpl tableRead) {
      super(tableRead);
      readRequest.setRows(RowSet.getDefaultInstance());
    }

    @Override
    protected List<Row> parentDataTypeToDataType(final List<Row> rows) {
      return rows;
    }

    @Override
    public RowsReadImpl addKeys(Collection<String> rowKeys) {
      final Set<ByteString> iterator =
          rowKeys.stream().map(ByteString::copyFromUtf8).collect(Collectors.toSet());
      readRequest.setRows(readRequest.getRowsBuilder().addAllRowKeys(iterator));
      return this;
    }

    @Override
    public RowsReadImpl limit(final long limit) {
      readRequest.setRowsLimit(limit);
      return this;
    }

    @Override
    public RowsReadImpl addRowRangeOpen(final ByteString startKeyOpen,
                                        final ByteString endKeyOpen) {
      final RowRange.Builder rowRange =
          RowRange.newBuilder().setStartKeyOpen(startKeyOpen).setEndKeyOpen(endKeyOpen);
      readRequest.setRows(readRequest.getRowsBuilder().addRowRanges(rowRange));
      return this;
    }

    @Override
    public RowsReadImpl addRowRangeClosed(final ByteString startKeyClosed,
                                          final ByteString endKeyClosed) {
      final RowRange.Builder rowRange =
          RowRange.newBuilder().setStartKeyClosed(startKeyClosed).setEndKeyClosed(endKeyClosed);
      readRequest.setRows(readRequest.getRowsBuilder().addRowRanges(rowRange));
      return this;
    }

    @Override
    public RowRead.RowReadImpl withinRow() {
      return new RowRead.RowReadImpl(this);
    }

    @Override
    public ResultScanner<Row> execute() {
      return getClient().readRows(readRequest().build());
    }
  }
}
