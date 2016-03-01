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

package com.spotify.bigtable.sample;

import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;

import java.util.List;

public class BigtableSampleRowKeysImpl extends BigtableTable implements BigtableSampleRowKeys {

  private final SampleRowKeysRequest.Builder sampleRowKeysRequest;

  public BigtableSampleRowKeysImpl(Bigtable bigtable, String table) {
    super(bigtable, table);
    sampleRowKeysRequest = SampleRowKeysRequest.newBuilder().setTableName(getFullTableName());
  }

  @Override
  public List<SampleRowKeysResponse> execute() {
    return bigtable.getSession().getDataClient().sampleRowKeys(sampleRowKeysRequest.build());
  }

  @Override
  public ListenableFuture<List<SampleRowKeysResponse>> executeAsync() {
    return bigtable.getSession().getDataClient().sampleRowKeysAsync(sampleRowKeysRequest.build());
  }

  @VisibleForTesting
  SampleRowKeysRequest.Builder getSampleRowKeysRequest() {
    return sampleRowKeysRequest;
  }
}
