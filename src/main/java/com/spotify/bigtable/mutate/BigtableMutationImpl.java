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

package com.spotify.bigtable.mutate;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.repackaged.com.google.protobuf.ServiceException;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;
import com.spotify.bigtable.ShadedListenableFutureWrapper;

import java.util.Optional;

public class BigtableMutationImpl extends BigtableTable implements BigtableMutation {

  private final MutateRowRequest.Builder mutateRowRequest;

  public BigtableMutationImpl(final Bigtable bigtable, final String table, final String row) {
    super(bigtable, table);
    this.mutateRowRequest = MutateRowRequest.newBuilder()
            .setTableName(getFullTableName())
            .setRowKey(ByteString.copyFromUtf8(row));
  }

  @Override
  public MutateRowResponse execute() throws ServiceException {
    return bigtable.getSession().getDataClient().mutateRow(mutateRowRequest.build());
  }

  @Override
  public ListenableFuture<MutateRowResponse> executeAsync() {
    return new ShadedListenableFutureWrapper<>(
        bigtable.getSession().getDataClient().mutateRowAsync(mutateRowRequest.build()));
  }

  @Override
  public BigtableMutation deleteRow() {
    mutateRowRequest.addMutations(Mutation.newBuilder().setDeleteFromRow(Mutation.DeleteFromRow.newBuilder()));
    return this;
  }

  @Override
  public BigtableMutation deleteColumnFamily(String columnFamily) {
    final Mutation.DeleteFromFamily.Builder deleteFromFamily = Mutation.DeleteFromFamily.newBuilder().setFamilyName(columnFamily);
    mutateRowRequest.addMutations(Mutation.newBuilder().setDeleteFromFamily(deleteFromFamily));
    return this;
  }

  @Override
  public BigtableMutation deleteColumn(final String column) {
    final String[] split = column.split(":", 2);
    return deleteColumn(split[0], split[1]);
  }

  @Override
  public BigtableMutation deleteColumn(String columnFamily, String columnQualifier) {
    return deleteCellsFromColumn(columnFamily, columnQualifier, Optional.empty(), Optional.empty());
  }

  @Override
  public BigtableMutation deleteCellsFromColumn(final String columnFamily,
                                                final String columnQualifier,
                                                final Optional<Long> startTimestampMicros,
                                                final Optional<Long> endTimestampMicros) {
    final TimestampRange.Builder timestampRange = TimestampRange.newBuilder();
    startTimestampMicros.ifPresent(timestampRange::setStartTimestampMicros);
    endTimestampMicros.ifPresent(timestampRange::setEndTimestampMicros);

    final Mutation.DeleteFromColumn.Builder deleteFromColumn = Mutation.DeleteFromColumn.newBuilder()
            .setFamilyName(columnFamily)
            .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
            .setTimeRange(timestampRange);

    mutateRowRequest.addMutations(Mutation.newBuilder().setDeleteFromColumn(deleteFromColumn));
    return this;
  }

  @Override
  public BigtableMutation write(final String column, final ByteString value) {
    final String[] split = column.split(":", 2);
    return write(split[0], split[1], value);
  }

  @Override
  public BigtableMutation write(final String column, final ByteString value, final long timestampMicros) {
    final String[] split = column.split(":", 2);
    return write(split[0], split[1], value, timestampMicros);
  }

  @Override
  public BigtableMutation write(final String columnFamily, final String columnQualifier, final ByteString value) {
    final Mutation.SetCell.Builder setCell = setCell(columnFamily, columnQualifier, value, Optional.empty());
    mutateRowRequest.addMutations(Mutation.newBuilder().setSetCell(setCell));
    return this;
  }

  @Override
  public BigtableMutation write(final String columnFamily, final String columnQualifier,
                                final ByteString value, final long timestampMicros) {
    final Mutation.SetCell.Builder setCell =
            setCell(columnFamily, columnQualifier, value, Optional.of(timestampMicros));
    mutateRowRequest.addMutations(Mutation.newBuilder().setSetCell(setCell));
    return this;
  }

  @VisibleForTesting
  public MutateRowRequest.Builder getMutateRowRequest() {
    return mutateRowRequest;
  }

  private Mutation.SetCell.Builder setCell(final String columnFamily, final String columnQualifier,
                                           final ByteString value, final Optional<Long> timestampMicros) {
    final Mutation.SetCell.Builder setCell = Mutation.SetCell.newBuilder()
            .setFamilyName(columnFamily)
            .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
            .setValue(value);
    timestampMicros.ifPresent(setCell::setTimestampMicros);
    return setCell;
  }
}
