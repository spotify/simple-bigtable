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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;
import com.spotify.bigtable.read.ReadRow.RowMultiRead;
import com.spotify.bigtable.read.ReadRow.RowSingleRead;
import com.spotify.bigtable.read.ReadRows.RowsRead;
import com.spotify.bigtable.read.ReadRows.RowsReadImpl;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public interface TableRead {

  /**
   * Read from a single row with given key.
   *
   * @param row row key
   * @return Row Read implementation
   */
  RowSingleRead row(final String row);

  RowMultiRead rows(final Collection<String> rowKeys);

  RowsRead rows();


  class TableReadImpl extends BigtableTable implements TableRead, BigtableRead.Internal<List<Row>> {

    public TableReadImpl(final Bigtable bigtable, final String table) {
      super(bigtable, table);
    }

    public RowSingleRead.ReadImpl row(final String row) {
      final RowsReadImpl rowsRead = rows().addKeys(Collections.singleton(row)).limit(1);
      return new RowSingleRead.ReadImpl(rowsRead);
    }

    @Override
    public RowsReadImpl rows() {
      return new RowsReadImpl(this);
    }

    @Override
    public RowMultiRead.ReadImpl rows(final Collection<String> rowKeys) {
      final RowsReadImpl rowsRead = rows().addKeys(rowKeys).limit(rowKeys.size());
      return new RowMultiRead.ReadImpl(rowsRead);
    }

    public ReadRowsRequest.Builder readRequest() {
      return ReadRowsRequest.newBuilder().setTableName(getFullTableName());
    }

    public BigtableDataClient getClient() {
      return bigtable.getSession().getDataClient();
    }

    @Override
    public Function<List<Row>, List<Row>> toDataType() {
      return Function.identity();
    }
  }

  public static void main(String[] args) {
    Bigtable bigtable = null;

    bigtable.read("table")
        .rows()
        .addRowRangeOpen(ByteString.copyFromUtf8("aa"), ByteString.copyFromUtf8("bb").concat(ByteString.copyFrom(new byte[]{(byte) 0xFF})));
  }
}