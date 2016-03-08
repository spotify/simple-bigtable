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

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.RowFilter;
import com.google.common.util.concurrent.Futures;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static com.spotify.bigtable.read.AbstractBigtableRead.toExactMatchRegex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CellReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  CellRead.CellReadImpl cellRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    final RowRead.RowReadImpl rowRead = new RowRead.RowReadImpl(tableRead, "row");
    final FamilyRead.FamilyReadImpl familyRead = new FamilyRead.FamilyReadImpl(rowRead, "family");
    final ColumnRead.ColumnReadImpl columnRead = new ColumnRead.ColumnReadImpl(familyRead, "qualifier");
    cellRead = new CellRead.CellReadImpl(columnRead);
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
    assertEquals("row", readRequest.getRowKey().toStringUtf8());
    assertEquals(1, readRequest.getNumRowsLimit());
    assertTrue(readRequest.getFilter().getChain().getFiltersCount() >= 3);
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(toExactMatchRegex("qualifier"), readRequest.getFilter().getChain().getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
  }

  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), cellRead.getClient());
  }

  @Test
  public void testParentDataTypeToDataType() throws Exception {
    assertEquals(Optional.empty(), cellRead.parentDataTypeToDataType(Optional.empty()));
    assertEquals(Optional.empty(), cellRead.parentDataTypeToDataType(Optional.of(Column.getDefaultInstance())));

    final Cell cell = Cell.getDefaultInstance();
    final Column column = Column.newBuilder().addCells(cell).build();
    assertEquals(Optional.of(cell), cellRead.parentDataTypeToDataType(Optional.of(column)));
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadRequest(cellRead.readRequest());
    when(bigtableMock.getMockedDataClient().readRowsAsync(any()))
            .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    cellRead.executeAsync();

    verifyReadRequest(cellRead.readRequest()); // make sure execute did not change the read request
    verify(bigtableMock.getMockedDataClient()).readRowsAsync(cellRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }
}
