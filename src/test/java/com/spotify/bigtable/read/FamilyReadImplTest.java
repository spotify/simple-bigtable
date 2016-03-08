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

import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.spotify.bigtable.read.AbstractBigtableRead.toExactMatchAnyRegex;
import static com.spotify.bigtable.read.AbstractBigtableRead.toExactMatchRegex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class FamilyReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  FamilyRead.FamilyReadImpl familyRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    final RowRead.RowReadImpl rowRead = new RowRead.RowReadImpl(tableRead, "row");
    familyRead = new FamilyRead.FamilyReadImpl(rowRead, "family");
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
    assertEquals("row", readRequest.getRowKey().toStringUtf8());
    assertEquals(1, readRequest.getNumRowsLimit());
    assertTrue(readRequest.getFilter().getChain().getFiltersCount() >= 2);
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
  }

  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), familyRead.getClient());
  }

  @Test
  public void testParentDataTypeToDataType() throws Exception {
    assertEquals(Optional.empty(), familyRead.parentDataTypeToDataType(Optional.empty()));
    assertEquals(Optional.empty(), familyRead.parentDataTypeToDataType(Optional.of(Row.getDefaultInstance())));

    final Family family = Family.getDefaultInstance();
    final Row row = Row.newBuilder().addFamilies(family).build();
    assertEquals(Optional.of(family), familyRead.parentDataTypeToDataType(Optional.of(row)));
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadRequest(familyRead.readRequest());
    when(bigtableMock.getMockedDataClient().readRowsAsync(any()))
            .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    familyRead.executeAsync();

    verifyReadRequest(familyRead.readRequest()); // make sure execute did not change the read request
    verify(bigtableMock.getMockedDataClient()).readRowsAsync(familyRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testColumnQualifier() throws Exception {
    final ColumnRead.ColumnReadImpl read = (ColumnRead.ColumnReadImpl) familyRead.columnQualifier("qualifier");

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(3, chain.getFiltersCount());
    assertEquals(toExactMatchRegex("qualifier"), chain.getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testColumnQualifierRegex() throws Exception {
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) familyRead.columnQualifierRegex("regex");

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(3, chain.getFiltersCount());
    assertEquals("regex", chain.getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());

    // Need to make sure that the FamilyRead (ColumnsRead's parent) did not get the filters added as well.
    // This is necessary to reuse the objects.
    assertEquals(2, familyRead.readRequest().getFilter().getChain().getFiltersCount());
  }

  @Test
  public void testColumnsQualifiers() throws Exception {
    final List<String> qualifiers = ImmutableList.of("qualifier1", "qualifier2");
    final ColumnsRead.ColumnsReadImpl read = (ColumnsRead.ColumnsReadImpl) familyRead
            .columnsQualifiers(qualifiers);

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(3, chain.getFiltersCount());
    assertEquals(toExactMatchAnyRegex(qualifiers), chain.getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());

    // Need to make sure that the FamilyRead (ColumnsRead's parent) did not get the filters added as well.
    // This is necessary to reuse the objects.
    assertEquals(2, familyRead.readRequest().getFilter().getChain().getFiltersCount());
  }
}
