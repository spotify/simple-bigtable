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

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.DeleteFromColumn;
import com.google.bigtable.v1.Mutation.DeleteFromFamily;
import com.google.bigtable.v1.Mutation.DeleteFromRow;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.bigtable.v1.TimestampRange;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;
import com.spotify.bigtable.Bigtable;
import com.spotify.bigtable.BigtableTable;

import java.util.Optional;

public interface BigtableMutation {

  /**
   * Execute the row mutations synchronously. 
   *
   * @return Empty protobuf object
   * @throws ServiceException on error
   */
  Empty execute() throws ServiceException;

  /**
   * Execute the row mutations synchronously.
   *
   * @return Future of Empty protobuf object. Failed future on error.
   */
  ListenableFuture<Empty> executeAsync();

  /**
   * Delete the entire row.
   * @return BigtableMutation
   */
  BigtableMutation deleteRow();

  /**
   * Delete column family within row.
   * @param columnFamily column family name
   * @return BigtableMutation
   */
  BigtableMutation deleteColumnFamily(final String columnFamily);

  /**
   * Delete single column within row.
   * @param column column family and column qualifier separated by ':'
   * @return BigtableMutation
   */
  BigtableMutation deleteColumn(final String column);

  /**
   * Delete single column within row.
   * @param columnFamily column family
   * @param columnQualifer column qualifier
   * @return BigtableMutation
   */
  BigtableMutation deleteColumn(final String columnFamily, final String columnQualifer);

  /**
   * Delete cells within a column and a certain timestamp range
   * @param columnFamily column family
   * @param columnQualifier column qualifier
   * @param startTimestampMicros optional start of timestamp range
   * @param endTimestampMicros optional end of timestamp range
   * @return BigtableMutation
   */
  BigtableMutation deleteCellsFromColumn(final String columnFamily,
                                         final String columnQualifier,
                                         final Optional<Long> startTimestampMicros,
                                         final Optional<Long> endTimestampMicros);

  /**
   * Write a value.
   * @param column column (family and qualifier separated by ':')
   * @param value  value
   * @return BigtableMutation
   */
  BigtableMutation write(final String column, final ByteString value);

  /**
   * Write a value with an explicit timestamp.
   * @param column          column (family and qualifier separated by ':')
   * @param value           value
   * @param timestampMicros timestamp (in microseconds)
   * @return BigtableMutation
   */
  BigtableMutation write(final String column, final ByteString value, final long timestampMicros);

  /**
   * Write a value.
   * @param columnFamily    column family
   * @param columnQualifier column qualifier
   * @param value           value
   * @return BigtableMutation
   */
  BigtableMutation write(final String columnFamily, final String columnQualifier, final ByteString value);

  /**
   * Write a value with an explicit timestamp.
   * @param columnFamily    column family
   * @param columnQualifier column qualifier
   * @param value           value
   * @param timestampMicros timestamp (in microseconds)
   * @return BigtableMutation
   */
  BigtableMutation write(final String columnFamily, final String columnQualifier,
                         final ByteString value, final long timestampMicros);

  class BigtableMutationImpl extends BigtableTable implements BigtableMutation {

    private final MutateRowRequest.Builder mutateRowRequest;

    public BigtableMutationImpl(final Bigtable bigtable, final String table, final String row) {
      super(bigtable, table);
      this.mutateRowRequest = MutateRowRequest.newBuilder()
              .setTableName(getFullTableName())
              .setRowKey(ByteString.copyFromUtf8(row));
    }

    @Override
    public Empty execute() throws ServiceException {
      return bigtable.getSession().getDataClient().mutateRow(mutateRowRequest.build());
    }

    @Override
    public ListenableFuture<Empty> executeAsync() {
      return bigtable.getSession().getDataClient().mutateRowAsync(mutateRowRequest.build());
    }

    @Override
    public BigtableMutation deleteRow() {
      mutateRowRequest.addMutations(Mutation.newBuilder().setDeleteFromRow(DeleteFromRow.newBuilder()));
      return this;
    }

    @Override
    public BigtableMutation deleteColumnFamily(String columnFamily) {
      final DeleteFromFamily.Builder deleteFromFamily = DeleteFromFamily.newBuilder().setFamilyName(columnFamily);
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

      final DeleteFromColumn.Builder deleteFromColumn = DeleteFromColumn.newBuilder()
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
      final SetCell.Builder setCell = setCell(columnFamily, columnQualifier, value, Optional.empty());
      mutateRowRequest.addMutations(Mutation.newBuilder().setSetCell(setCell));
      return this;
    }

    @Override
    public BigtableMutation write(final String columnFamily, final String columnQualifier,
                                  final ByteString value, final long timestampMicros) {
      final SetCell.Builder setCell = setCell(columnFamily, columnQualifier, value, Optional.of(timestampMicros));
      mutateRowRequest.addMutations(Mutation.newBuilder().setSetCell(setCell));
      return this;
    }
    
    private SetCell.Builder setCell(final String columnFamily, final String columnQualifier,
                                    final ByteString value, final Optional<Long> timestampMicros) {
      final SetCell.Builder setCell = SetCell.newBuilder()
              .setFamilyName(columnFamily)
              .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
              .setValue(value);
      timestampMicros.ifPresent(setCell::setTimestampMicros);
      return setCell;
    }
  }
}
