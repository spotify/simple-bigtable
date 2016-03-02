package com.spotify.bigtable.sample;

import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface BigtableSampleRowKeys {

  List<SampleRowKeysResponse> execute();

  ListenableFuture<List<SampleRowKeysResponse>> executeAsync();

}
