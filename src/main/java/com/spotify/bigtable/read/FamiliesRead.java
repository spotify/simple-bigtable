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
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

public interface FamiliesRead extends BigtableRead<List<Family>> {

  class FamiliesReadImpl implements FamiliesRead, BigtableRead.Internal<List<Family>> {

    private final BigtableRead.Internal<Optional<Row>> row;
    private final ReadRowsRequest.Builder readRequest;

    public FamiliesReadImpl(final BigtableRead.Internal<Optional<Row>> row) {
      this.row = row;
      this.readRequest = row.readRequest();
    }

    public FamiliesReadImpl(final BigtableRead.Internal<Optional<Row>> row, final ReadRowsRequest.Builder readRequest) {
      this.row = row;
      this.readRequest = readRequest;
    }

    @Override
    public ReadRowsRequest.Builder readRequest() {
      return readRequest;
    }

    @Override
    public BigtableDataClient getClient() {
      return row.getClient();
    }

    @Override
    public List<Family> toDataType(final List<Row> rows) {
      return row.toDataType(rows).map(Row::getFamiliesList).orElse(Lists.newArrayList());
    }

    @Override
    public ListenableFuture<List<Family>> executeAsync() {
      return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
    }
  }

}
