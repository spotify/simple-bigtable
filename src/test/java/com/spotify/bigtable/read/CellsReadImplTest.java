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
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.bigtable.v2.ValueRange;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
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

public class CellsReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  CellsRead.CellsReadImpl cellsRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    final RowRead.RowReadImpl rowRead = new RowRead.RowReadImpl(tableRead, "row");
    final FamilyRead.FamilyReadImpl familyRead = new FamilyRead.FamilyReadImpl(rowRead, "family");
    final ColumnRead.ColumnReadImpl columnRead = new ColumnRead.ColumnReadImpl(familyRead, "qualifier");
    cellsRead = new CellsRead.CellsReadImpl(columnRead);
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
    assertEquals("row", readRequest.getRows().getRowKeys(0).toStringUtf8());
    assertEquals(1, readRequest.getRows().getRowKeysCount());
    assertEquals(0, readRequest.getRows().getRowRangesCount());
    assertEquals(1, readRequest.getRowsLimit());
    assertTrue(readRequest.getFilter().getChain().getFiltersCount() >= 3);
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(toExactMatchRegex("qualifier"), readRequest.getFilter().getChain().getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
  }
  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), cellsRead.getClient());
  }

  @Test
  public void testParentDataTypeToDataType() throws Exception {
    assertEquals(Lists.newArrayList(), cellsRead.parentDataTypeToDataType(Optional.empty()));
    assertEquals(Lists.newArrayList(), cellsRead.parentDataTypeToDataType(Optional.of(Column.getDefaultInstance())));

    final Cell cell = Cell.getDefaultInstance();
    final Column column = Column.newBuilder().addCells(cell).build();
    assertEquals(ImmutableList.of(cell), cellsRead.parentDataTypeToDataType(Optional.of(column)));
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadRequest(cellsRead.readRequest());
    when(bigtableMock.getMockedDataClient().readRowsAsync(any()))
            .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    cellsRead.executeAsync();

    verifyReadRequest(cellsRead.readRequest()); // make sure execute did not change the read request
    verify(bigtableMock.getMockedDataClient()).readRowsAsync(cellsRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testLatest() throws Exception {
    // Test Plain Latest
    CellRead.CellReadImpl cellRead = (CellRead.CellReadImpl) this.cellsRead.latest();

    ReadRowsRequest.Builder readRequest = cellRead.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(1, readRequest.getFilter().getChain().getFilters(3).getCellsPerColumnLimitFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());

    // Test A Filter Then A Latest
    final TimestampRange timestampRange = TimestampRange.newBuilder().setStartTimestampMicros(12345).build();
    cellRead = (CellRead.CellReadImpl) this.cellsRead
            .startTimestampMicros(timestampRange.getStartTimestampMicros())
            .valueRegex(ByteString.copyFromUtf8("regex"))
            .latest();

    readRequest = cellRead.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(6, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(timestampRange, readRequest.getFilter().getChain().getFilters(3).getTimestampRangeFilter());
    assertEquals("regex", readRequest.getFilter().getChain().getFilters(4).getValueRegexFilter().toStringUtf8());
    assertEquals(1, readRequest.getFilter().getChain().getFilters(5).getCellsPerColumnLimitFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());

    // Need to make sure that the ColumnRead (CellsRead's parent) did not get the filters added as well.
    // This is necessary to reuse the objects.
    assertEquals(3, cellsRead.parentRead.readRequest().getFilter().getChain().getFiltersCount());
  }

  @Test
  public void testLimit() throws Exception {
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead.limit(10);

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(10, readRequest.getFilter().getChain().getFilters(3).getCellsPerColumnLimitFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testStartTimestampMicros() throws Exception {
    final TimestampRange timestampRange = TimestampRange.newBuilder().setStartTimestampMicros(12345).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead
            .startTimestampMicros(timestampRange.getStartTimestampMicros());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(timestampRange, readRequest.getFilter().getChain().getFilters(3).getTimestampRangeFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testEndTimestampMicros() throws Exception {
    final TimestampRange timestampRange = TimestampRange.newBuilder().setEndTimestampMicros(12345).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead
            .endTimestampMicros(timestampRange.getEndTimestampMicros());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(timestampRange, readRequest.getFilter().getChain().getFilters(3).getTimestampRangeFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testValueRegex() throws Exception {
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead.valueRegex(ByteString.copyFromUtf8("regex"));

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals("regex", readRequest.getFilter().getChain().getFilters(3).getValueRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testStartValueClosed() throws Exception {
    final ValueRange valueRange = ValueRange.newBuilder().setStartValueClosed(ByteString.copyFromUtf8("regex")).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead
            .startValueClosed(valueRange.getStartValueClosed());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(valueRange, readRequest.getFilter().getChain().getFilters(3).getValueRangeFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testStartValueOpen() throws Exception {
    final ValueRange valueRange = ValueRange.newBuilder().setStartValueOpen(ByteString.copyFromUtf8("regex")).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead
            .startValueOpen(valueRange.getStartValueOpen());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(valueRange, readRequest.getFilter().getChain().getFilters(3).getValueRangeFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testEndValueClosed() throws Exception {
    final ValueRange valueRange = ValueRange.newBuilder().setEndValueClosed(ByteString.copyFromUtf8("regex")).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead
            .endValueClosed(valueRange.getEndValueClosed());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(valueRange, readRequest.getFilter().getChain().getFilters(3).getValueRangeFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testEndValueOpen() throws Exception {
    final ValueRange valueRange = ValueRange.newBuilder().setEndValueOpen(ByteString.copyFromUtf8("regex")).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead
            .endValueOpen(valueRange.getEndValueOpen());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(4, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(valueRange, readRequest.getFilter().getChain().getFilters(3).getValueRangeFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testMultipleFilters() throws Exception {
    final TimestampRange startTimestampRange = TimestampRange.newBuilder().setStartTimestampMicros(12345).build();
    final TimestampRange endTimestampRange = TimestampRange.newBuilder().setEndTimestampMicros(12345).build();
    final CellsRead.CellsReadImpl read = (CellsRead.CellsReadImpl) cellsRead.limit(10)
            .startTimestampMicros(startTimestampRange.getStartTimestampMicros())
            .endTimestampMicros(endTimestampRange.getEndTimestampMicros())
            .valueRegex(ByteString.copyFromUtf8("regex"));

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(7, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(10, readRequest.getFilter().getChain().getFilters(3).getCellsPerColumnLimitFilter());
    assertEquals(startTimestampRange, readRequest.getFilter().getChain().getFilters(4).getTimestampRangeFilter());
    assertEquals(endTimestampRange, readRequest.getFilter().getChain().getFilters(5).getTimestampRangeFilter());
    assertEquals("regex", readRequest.getFilter().getChain().getFilters(6).getValueRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }
}
