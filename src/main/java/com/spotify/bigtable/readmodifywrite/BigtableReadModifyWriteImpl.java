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

package com.spotify.bigtable.readmodifywrite;

import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;

public class BigtableReadModifyWriteImpl
        extends BigtableTable
        implements BigtableReadModifyWrite, BigtableReadModifyWrite.Read {

  private final ReadModifyWriteRowRequest.Builder readModifyWriteRequest;
  private final ReadModifyWriteRule.Builder readModifyWriteRule;

  public BigtableReadModifyWriteImpl(final Bigtable bigtable, final String table, final String row) {
    super(bigtable, table);
    this.readModifyWriteRequest = ReadModifyWriteRowRequest.newBuilder()
            .setTableName(getFullTableName())
            .setRowKey(ByteString.copyFromUtf8(row));
    this.readModifyWriteRule = ReadModifyWriteRule.newBuilder();
  }

  @Override
  public ReadModifyWriteRowResponse execute() {
    return bigtable.getSession().getDataClient().readModifyWriteRow(readModifyWriteRequest.build());
  }

  @Override
  public ListenableFuture<ReadModifyWriteRowResponse> executeAsync() {
    return (ListenableFuture<ReadModifyWriteRowResponse>)
        bigtable.getSession().getDataClient().readModifyWriteRowAsync(readModifyWriteRequest.build());
  }

  @Override
  public Read read(final String column) {
    final String[] split = column.split(":", 2);
    return read(split[0], split[1]);
  }

  @Override
  public Read read(final String columnFamily, final String columnQualifier) {
    readModifyWriteRule
            .setFamilyName(columnFamily)
            .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier));
    return this;
  }

  @Override
  public BigtableReadModifyWrite thenAppendValue(final ByteString value) {
    readModifyWriteRule.setAppendValue(value);
    return addRule(readModifyWriteRule);
  }

  @Override
  public BigtableReadModifyWrite thenIncrementAmount(final long incBy) {
    readModifyWriteRule.setIncrementAmount(incBy);
    return addRule(readModifyWriteRule);
  }

  private BigtableReadModifyWrite addRule(final ReadModifyWriteRule.Builder rule) {
    readModifyWriteRequest.addRules(rule);
    rule.clear();
    return this;
  }

  @VisibleForTesting
  public ReadModifyWriteRowRequest.Builder getReadModifyWriteRequest() {
    return readModifyWriteRequest;
  }

  @VisibleForTesting
  ReadModifyWriteRule.Builder getReadModifyWriteRule() {
    return readModifyWriteRule;
  }
}
