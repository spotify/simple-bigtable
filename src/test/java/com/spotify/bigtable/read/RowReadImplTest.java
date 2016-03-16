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

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RowReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  RowRead.RowReadImpl rowRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    rowRead = new RowRead.RowReadImpl(tableRead, "row");
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
    assertEquals("row", readRequest.getRowKey().toStringUtf8());
    assertEquals(1, readRequest.getNumRowsLimit());
  }

  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), rowRead.getClient());
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadRequest(rowRead.readRequest());
    when(bigtableMock.getMockedDataClient().readRowsAsync(any()))
            .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    rowRead.executeAsync();
    verify(bigtableMock.getMockedDataClient()).readRowsAsync(rowRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testToDataType() throws Exception {
    assertFalse(rowRead.toDataType(ImmutableList.of()).isPresent());

    final Row row = Row.getDefaultInstance();
    assertEquals(row, rowRead.toDataType(ImmutableList.of(row)).get());
  }

  @Test
  public void testFamily() throws Exception {
    FamilyRead.FamilyReadImpl family = (FamilyRead.FamilyReadImpl) rowRead.family("family");

    ReadRowsRequest.Builder readRequest = family.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());

    family = (FamilyRead.FamilyReadImpl) rowRead.family(ByteString.copyFromUtf8("family"));

    readRequest = family.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(ByteString.copyFromUtf8("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilterBytes());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testFamilyRegex() throws Exception {
    FamiliesRead.FamiliesReadImpl families = (FamiliesRead.FamiliesReadImpl) rowRead.familyRegex("family-regex");

    ReadRowsRequest.Builder readRequest = families.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals("family-regex", readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());

    families = (FamiliesRead.FamiliesReadImpl) rowRead.familyRegex(ByteString.copyFromUtf8("family-regex"));

    readRequest = families.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals("family-regex", readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testFamilies() throws Exception {
    final List<String> familyNames = ImmutableList.of("family1", "family2");
    final FamiliesRead.FamiliesReadImpl families = (FamiliesRead.FamiliesReadImpl) rowRead.families(familyNames);

    final ReadRowsRequest.Builder readRequest = families.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchAnyRegex(familyNames), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testColumn() throws Exception {
    final ColumnRead.ColumnReadImpl column = (ColumnRead.ColumnReadImpl) rowRead.column("family:qualifier");

    final ReadRowsRequest.Builder readRequest = column.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(3, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(AbstractBigtableRead.toExactMatchRegex("qualifier"), readRequest.getFilter().getChain().getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build()
    );
  }

  @Test
  public void testColumns() throws Exception {
    final ColumnsRead.ColumnsReadImpl columns = (ColumnsRead.ColumnsReadImpl) rowRead.columns();

    final ReadRowsRequest.Builder readRequest = columns.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter());
  }

  @Test
  public void testColumnsList() throws Exception {
    final ColumnsRead.ColumnsReadImpl columns = (ColumnsRead.ColumnsReadImpl) rowRead.columns(Lists.newArrayList("family:qualifier1", "family:qualifier2"));

    final ReadRowsRequest.Builder readRequest = columns.readRequest();
    verifyReadRequest(readRequest);
    final RowFilter.Chain chain = readRequest.getFilter().getChain();
    assertEquals(3, chain.getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), chain.getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchRegex("family"), chain.getFilters(1).getFamilyNameRegexFilter());
    assertEquals(AbstractBigtableRead.toExactMatchAnyRegex(Lists.newArrayList("qualifier1", "qualifier2")), chain.getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }
}
