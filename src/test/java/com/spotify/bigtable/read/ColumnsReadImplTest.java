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
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ColumnsReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  ColumnsRead.ColumnsReadImpl columnsRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    final RowRead.RowReadImpl rowRead = new RowRead.RowReadImpl(tableRead, "row");
    columnsRead = new ColumnsRead.ColumnsReadImpl(rowRead);
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
    assertEquals("row", readRequest.getRowKey().toStringUtf8());
    assertEquals(1, readRequest.getNumRowsLimit());
  }

  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), columnsRead.getClient());
  }

  @Test
  public void testParentDataTypeToDataType() throws Exception {
    assertEquals(Lists.newArrayList(), columnsRead.parentDataTypeToDataType(Optional.empty()));
    assertEquals(Lists.newArrayList(), columnsRead.parentDataTypeToDataType(Optional.of(Row.getDefaultInstance())));

    final Column column = Column.getDefaultInstance();
    final Family family = Family.newBuilder().addColumns(column).build();
    final Row row = Row.newBuilder().addFamilies(family).build();
    assertEquals(ImmutableList.of(column), columnsRead.parentDataTypeToDataType(Optional.of(row)));
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadRequest(columnsRead.readRequest());
    when(bigtableMock.getMockedDataClient().readRowsAsync(any()))
            .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    columnsRead.executeAsync();

    verifyReadRequest(columnsRead.readRequest()); // make sure execute did not change the read request
    verify(bigtableMock.getMockedDataClient()).readRowsAsync(columnsRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testFamilyName() throws Exception {
    final ColumnRange columnRange = ColumnRange.newBuilder().setFamilyName("family").build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) this.columnsRead
            .familyName(columnRange.getFamilyName());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(2, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(columnRange, chain.getFilters(1).getColumnRangeFilter());
  }

  @Test
  public void testFamilyNameBytes() throws Exception {
    final ColumnRange columnRange = ColumnRange.newBuilder().setFamilyNameBytes(ByteString.copyFromUtf8("family")).build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) this.columnsRead
            .familyName(columnRange.getFamilyName());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(2, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(columnRange, chain.getFilters(1).getColumnRangeFilter());
  }

  @Test
  public void testStartQualifierInclusive() throws Exception {
    final ColumnRange columnRange = ColumnRange.newBuilder().setStartQualifierInclusive(ByteString.copyFromUtf8("qual")).build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) columnsRead
            .startQualifierInclusive(columnRange.getStartQualifierInclusive());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(2, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(columnRange, chain.getFilters(1).getColumnRangeFilter());
  }

  @Test
  public void testStartQualifierExclusive() throws Exception {
    final ColumnRange columnRange = ColumnRange.newBuilder().setStartQualifierExclusive(ByteString.copyFromUtf8("qual")).build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) columnsRead
            .startQualifierExclusive(columnRange.getStartQualifierExclusive());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(2, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(columnRange, chain.getFilters(1).getColumnRangeFilter());
  }

  @Test
  public void testEndQualifierInclusive() throws Exception {
    final ColumnRange columnRange = ColumnRange.newBuilder().setEndQualifierInclusive(ByteString.copyFromUtf8("qual")).build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) columnsRead
            .endQualifierInclusive(columnRange.getEndQualifierInclusive());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(2, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(columnRange, chain.getFilters(1).getColumnRangeFilter());
  }

  @Test
  public void testEndQualifierExclusive() throws Exception {
    final ColumnRange columnRange = ColumnRange.newBuilder().setEndQualifierExclusive(ByteString.copyFromUtf8("qual")).build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) columnsRead
            .endQualifierExclusive(columnRange.getEndQualifierExclusive());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(2, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(columnRange, chain.getFilters(1).getColumnRangeFilter());
  }

  @Test
  public void testMultipleFilters() throws Exception {
    final ColumnRange familyRange = ColumnRange.newBuilder().setFamilyName("family").build();
    final ColumnRange startQualRange = ColumnRange.newBuilder().setStartQualifierInclusive(ByteString.copyFromUtf8("start")).build();
    final ColumnRange endQualRange = ColumnRange.newBuilder().setEndQualifierExclusive(ByteString.copyFromUtf8("end")).build();
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) this.columnsRead.familyName(familyRange.getFamilyName())
            .startQualifierInclusive(startQualRange.getStartQualifierInclusive())
            .endQualifierExclusive(endQualRange.getEndQualifierExclusive());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(4, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(familyRange, chain.getFilters(1).getColumnRangeFilter());
    assertEquals(startQualRange, chain.getFilters(2).getColumnRangeFilter());
    assertEquals(endQualRange, chain.getFilters(3).getColumnRangeFilter());
  }
}
