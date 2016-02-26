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

import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRule;
import com.google.bigtable.v1.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;

public interface BigtableReadModifyWrite {

  Row execute();

  ListenableFuture<Row> executeAsync();

  BigtableReadModifyWrite.Read read(final String column);

  BigtableReadModifyWrite.Read read(final String columnFamily, final String columnQualifier);

  interface Read {

    BigtableReadModifyWrite thenAppendValue(final ByteString value);

    BigtableReadModifyWrite thenIncrementAmount(final long incBy);

  }

  class BigtableReadModifyWriteImpl extends BigtableTable implements BigtableReadModifyWrite {

    private final ReadModifyWriteRowRequest.Builder readModifyWrite;

    public BigtableReadModifyWriteImpl(final Bigtable bigtable, final String table, final String row) {
      super(bigtable, table);
      this.readModifyWrite = ReadModifyWriteRowRequest.newBuilder()
              .setTableName(getFullTableName())
              .setRowKey(ByteString.copyFromUtf8(row));
    }

    @Override
    public Row execute() {
      return bigtable.getSession().getDataClient().readModifyWriteRow(readModifyWrite.build());
    }

    @Override
    public ListenableFuture<Row> executeAsync() {
      return bigtable.getSession().getDataClient().readModifyWriteRowAsync(readModifyWrite.build());
    }

    @Override
    public Read read(final String column) {
      final String[] split = column.split(":", 2);
      return read(split[0], split[1]);
    }

    @Override
    public Read read(final String columnFamily, final String columnQualifier) {
      final ReadModifyWriteRule.Builder rule = ReadModifyWriteRule.newBuilder()
              .setFamilyName(columnFamily)
              .setColumnQualifier(ByteString.copyFromUtf8(columnFamily));
      return new BigtableReadModifyWriteReadImpl(this, rule);
    }

    protected BigtableReadModifyWrite addRule(final ReadModifyWriteRule.Builder rule) {
      readModifyWrite.addRules(rule);
      return this;
    }
  }

  class BigtableReadModifyWriteReadImpl implements BigtableReadModifyWrite.Read {

    private final BigtableReadModifyWriteImpl bigtableReadModifyWrite;
    private final ReadModifyWriteRule.Builder readModifyWriteRule;

    public BigtableReadModifyWriteReadImpl(final BigtableReadModifyWriteImpl bigtableReadModifyWrite,
                                           final ReadModifyWriteRule.Builder readModifyWriteRule) {
      this.bigtableReadModifyWrite = bigtableReadModifyWrite;
      this.readModifyWriteRule = readModifyWriteRule;
    }

    @Override
    public BigtableReadModifyWrite thenAppendValue(final ByteString value) {
      readModifyWriteRule.setAppendValue(value);
      return bigtableReadModifyWrite.addRule(readModifyWriteRule);
    }

    @Override
    public BigtableReadModifyWrite thenIncrementAmount(final long incBy) {
      readModifyWriteRule.setIncrementAmount(incBy);
      return bigtableReadModifyWrite.addRule(readModifyWriteRule);
    }
  }
}
