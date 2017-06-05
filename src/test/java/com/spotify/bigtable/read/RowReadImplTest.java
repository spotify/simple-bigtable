/*-
 * -\-\-
 * simple-bigtable
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.spotify.bigtable.BigtableMock;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class RowReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  ReadRow.RowSingleRead.ReadImpl rowRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    rowRead = tableRead.row("row");
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
    assertEquals("row", readRequest.getRows().getRowKeys(0).toStringUtf8());
    assertEquals(1, readRequest.getRows().getRowKeysCount());
    assertEquals(0, readRequest.getRows().getRowRangesCount());
    assertEquals(1, readRequest.getRowsLimit());
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
    assertFalse(rowRead.toDataType().apply(ImmutableList.of()).isPresent());

    final Row row = Row.getDefaultInstance();
    assertEquals(row, rowRead.toDataType().apply(ImmutableList.of(row)).get());
  }

  @Test
  public void testFamily() throws Exception {
    final ReadFamily.FamilyWithinRowRead.ReadImpl family =
        (ReadFamily.FamilyWithinRowRead.ReadImpl) rowRead.family("family");

    final ReadRowsRequest.Builder readRequest = family.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testFamilyRegex() throws Exception {
    final ReadFamilies.FamiliesWithinRowRead.ReadImpl families =
        (ReadFamilies.FamiliesWithinRowRead.ReadImpl) rowRead.familyRegex("family-regex");

    final ReadRowsRequest.Builder readRequest = families.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals("family-regex", readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testFamilies() throws Exception {
    final List<String> familyNames = ImmutableList.of("family1", "family2");
    final ReadFamilies.FamiliesWithinRowRead.ReadImpl families =
        (ReadFamilies.FamiliesWithinRowRead.ReadImpl) rowRead.families(familyNames);

    final ReadRowsRequest.Builder readRequest = families.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(2, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchAnyRegex(familyNames), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build());
  }

  @Test
  public void testColumn() throws Exception {
    final ReadColumn.ColumnWithinFamilyRead.ReadImpl column =
        (ReadColumn.ColumnWithinFamilyRead.ReadImpl) rowRead.column("family:qualifier");

    final ReadRowsRequest.Builder readRequest = column.readRequest();
    verifyReadRequest(readRequest);
    assertEquals(3, readRequest.getFilter().getChain().getFiltersCount());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().getChain().getFilters(0));
    assertEquals(AbstractBigtableRead.toExactMatchRegex("family"), readRequest.getFilter().getChain().getFilters(1).getFamilyNameRegexFilter());
    assertEquals(AbstractBigtableRead.toExactMatchRegex("qualifier"), readRequest.getFilter().getChain().getFilters(2).getColumnQualifierRegexFilter().toStringUtf8());
    assertEquals(RowFilter.getDefaultInstance(), readRequest.getFilter().toBuilder().clearChain().build()
    );
  }
}
