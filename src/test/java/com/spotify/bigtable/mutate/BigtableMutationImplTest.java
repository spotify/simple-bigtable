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
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class BigtableMutationImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  BigtableMutationImpl bigtableMutation;

  @Before
  public void setUp() throws Exception {
    bigtableMutation = new BigtableMutationImpl(bigtableMock, "table", "row");
  }

  private void verifyGetMutateRowRequest() throws Exception {
    final MutateRowRequest.Builder mutateRowRequest = bigtableMutation.getMutateRowRequest();
    assertEquals(bigtableMock.getFullTableName("table"), mutateRowRequest.getTableName());
    assertEquals("row", mutateRowRequest.getRowKey().toStringUtf8());
  }

  @Test
  public void testExecute() throws Exception {
    verifyGetMutateRowRequest();
    bigtableMutation.execute();
    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyGetMutateRowRequest();
    bigtableMutation.executeAsync();
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testDeleteRow() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl = (BigtableMutationImpl) this.bigtableMutation.deleteRow();
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(1, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());
    final Mutation mutation = bigtableMutationImpl.getMutateRowRequest().getMutations(0);
    assertEquals(Mutation.MutationCase.DELETE_FROM_ROW, mutation.getMutationCase());
    assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
    assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
    assertEquals(Mutation.DeleteFromColumn.getDefaultInstance(), mutation.getDeleteFromColumn());
    assertEquals(Mutation.SetCell.getDefaultInstance(), mutation.getSetCell());

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testDeleteColumnFamily() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl =
            (BigtableMutationImpl) this.bigtableMutation.deleteColumnFamily("family");
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(1, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());
    final Mutation mutation = bigtableMutationImpl.getMutateRowRequest().getMutations(0);
    assertEquals(Mutation.MutationCase.DELETE_FROM_FAMILY, mutation.getMutationCase());
    assertEquals("family", mutation.getDeleteFromFamily().getFamilyName());
    assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
    assertEquals(Mutation.DeleteFromColumn.getDefaultInstance(), mutation.getDeleteFromColumn());
    assertEquals(Mutation.SetCell.getDefaultInstance(), mutation.getSetCell());

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testDeleteColumn() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl =
            (BigtableMutationImpl) this.bigtableMutation.deleteColumn("family:qualifier");
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(1, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());
    final Mutation mutation = bigtableMutationImpl.getMutateRowRequest().getMutations(0);
    assertEquals(Mutation.MutationCase.DELETE_FROM_COLUMN, mutation.getMutationCase());
    assertEquals("family", mutation.getDeleteFromColumn().getFamilyName());
    assertEquals("qualifier", mutation.getDeleteFromColumn().getColumnQualifier().toStringUtf8());
    assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
    assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
    assertEquals(Mutation.SetCell.getDefaultInstance(), mutation.getSetCell());

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testDeleteColumnFamilyAndQualifier() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl =
            (BigtableMutationImpl) this.bigtableMutation.deleteColumn("family", "qualifier");
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(1, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());
    final Mutation mutation = bigtableMutationImpl.getMutateRowRequest().getMutations(0);
    assertEquals(Mutation.MutationCase.DELETE_FROM_COLUMN, mutation.getMutationCase());
    assertEquals("family", mutation.getDeleteFromColumn().getFamilyName());
    assertEquals("qualifier", mutation.getDeleteFromColumn().getColumnQualifier().toStringUtf8());
    assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
    assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
    assertEquals(Mutation.SetCell.getDefaultInstance(), mutation.getSetCell());

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testDeleteCellsFromColumn() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl = (BigtableMutationImpl) this.bigtableMutation
            .deleteCellsFromColumn("family", "qualifier", Optional.empty(), Optional.empty())
            .deleteCellsFromColumn("family", "qualifier", Optional.of(100L), Optional.of(999L));
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(2, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());

    Mutation mutation = bigtableMutationImpl.getMutateRowRequest().getMutations(0);
    assertEquals(Mutation.MutationCase.DELETE_FROM_COLUMN, mutation.getMutationCase());
    assertEquals("family", mutation.getDeleteFromColumn().getFamilyName());
    assertEquals("qualifier", mutation.getDeleteFromColumn().getColumnQualifier().toStringUtf8());
    assertEquals(TimestampRange.getDefaultInstance(), mutation.getDeleteFromColumn().getTimeRange());
    assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
    assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
    assertEquals(Mutation.SetCell.getDefaultInstance(), mutation.getSetCell());

    mutation = bigtableMutationImpl.getMutateRowRequest().getMutations(1);
    assertEquals(Mutation.MutationCase.DELETE_FROM_COLUMN, mutation.getMutationCase());
    assertEquals("family", mutation.getDeleteFromColumn().getFamilyName());
    assertEquals("qualifier", mutation.getDeleteFromColumn().getColumnQualifier().toStringUtf8());
    assertEquals(100L, mutation.getDeleteFromColumn().getTimeRange().getStartTimestampMicros());
    assertEquals(999L, mutation.getDeleteFromColumn().getTimeRange().getEndTimestampMicros());
    assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
    assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
    assertEquals(Mutation.SetCell.getDefaultInstance(), mutation.getSetCell());

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testWriteColumnValue() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl = (BigtableMutationImpl) this.bigtableMutation
            .write("family:qualifier", ByteString.copyFromUtf8("value"))
            .write("family", "qualifier", ByteString.copyFromUtf8("value"));
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(2, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());
    for (Mutation mutation : bigtableMutationImpl.getMutateRowRequest().getMutationsList()) {
      assertEquals(Mutation.MutationCase.SET_CELL, mutation.getMutationCase());
      assertEquals("family", mutation.getSetCell().getFamilyName());
      assertEquals("qualifier", mutation.getSetCell().getColumnQualifier().toStringUtf8());
      assertEquals("value", mutation.getSetCell().getValue().toStringUtf8());
      assertEquals(0L, mutation.getSetCell().getTimestampMicros());
      assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
      assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
      assertEquals(Mutation.DeleteFromColumn.getDefaultInstance(), mutation.getDeleteFromColumn());
    }

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testWriteColumnAndTimestamp() throws Exception {
    final BigtableMutationImpl bigtableMutationImpl = (BigtableMutationImpl) this.bigtableMutation
            .write("family:qualifier", ByteString.copyFromUtf8("value"), 100L)
            .write("family", "qualifier", ByteString.copyFromUtf8("value"), 100L);
    verifyGetMutateRowRequest();

    bigtableMutationImpl.execute();
    bigtableMutationImpl.executeAsync();

    assertEquals(2, bigtableMutationImpl.getMutateRowRequest().getMutationsCount());

    for (Mutation mutation : bigtableMutationImpl.getMutateRowRequest().getMutationsList()) {
      assertEquals(Mutation.MutationCase.SET_CELL, mutation.getMutationCase());
      assertEquals("family", mutation.getSetCell().getFamilyName());
      assertEquals("qualifier", mutation.getSetCell().getColumnQualifier().toStringUtf8());
      assertEquals("value", mutation.getSetCell().getValue().toStringUtf8());
      assertEquals(100L, mutation.getSetCell().getTimestampMicros());
      assertEquals(Mutation.DeleteFromRow.getDefaultInstance(), mutation.getDeleteFromRow());
      assertEquals(Mutation.DeleteFromFamily.getDefaultInstance(), mutation.getDeleteFromFamily());
      assertEquals(Mutation.DeleteFromColumn.getDefaultInstance(), mutation.getDeleteFromColumn());
    }

    verify(bigtableMock.getMockedDataClient()).mutateRow(bigtableMutation.getMutateRowRequest().build());
    verify(bigtableMock.getMockedDataClient()).mutateRowAsync(bigtableMutation.getMutateRowRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }
}
