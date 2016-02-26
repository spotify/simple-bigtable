package com.spotify.bigtable.sample;

import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;

import java.util.List;

public interface BigtableSampleRowKeys {

  List<SampleRowKeysResponse> execute();

  ListenableFuture<List<SampleRowKeysResponse>> executeAsync();

  class BigtableSampleRowKeysImpl extends BigtableTable implements BigtableSampleRowKeys {

    private final SampleRowKeysRequest.Builder sampleRowKeys;

    public BigtableSampleRowKeysImpl(Bigtable bigtable, String table) {
      super(bigtable, table);
      sampleRowKeys = SampleRowKeysRequest.newBuilder().setTableName(getFullTableName());
    }

    @Override
    public List<SampleRowKeysResponse> execute() {
      return bigtable.getSession().getDataClient().sampleRowKeys(sampleRowKeys.build());
    }

    @Override
    public ListenableFuture<List<SampleRowKeysResponse>> executeAsync() {
      return bigtable.getSession().getDataClient().sampleRowKeysAsync(sampleRowKeys.build());
    }
  }
}
