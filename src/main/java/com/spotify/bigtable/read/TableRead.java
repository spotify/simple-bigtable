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
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;

import java.util.List;

public interface TableRead {

  RowRead row(final String row);

  RowsRead rows();

  class TableReadImpl extends BigtableTable implements TableRead, BigtableRead.Internal<List<Row>> {

    public TableReadImpl(final Bigtable bigtable, final String table) {
      super(bigtable, table);
    }

    public RowRead row(final String row) {
      return new RowRead.RowReadImpl(this, row);
    }

    @Override
    public RowsRead rows() {
      return new RowsRead.RowsReadImpl(this);
    }

    public ReadRowsRequest.Builder readRequest() {
      return ReadRowsRequest.newBuilder().setTableName(getFullTableName());
    }

    public BigtableDataClient getClient() {
      return bigtable.getSession().getDataClient();
    }

    @Override
    public List<Row> toDataType(final List<Row> rows) {
      return rows;
    }
  }
}