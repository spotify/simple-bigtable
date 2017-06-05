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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RowsReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  ReadRows.RowsReadImpl rowsRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    rowsRead = tableRead.rows();
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
  }

  @Test
  public void testParentDataTypeToDataType() throws Exception {
    final List<Row> rows = ImmutableList.of(Row.getDefaultInstance());
    assertEquals(rows, rowsRead.toDataType().apply(rows));
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadRequest(rowsRead.readRequest());
    when(bigtableMock.getMockedDataClient().readRowsAsync(any()))
            .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    rowsRead.executeAsync();

    verifyReadRequest(rowsRead.readRequest()); // make sure execute did not change the read request
    verify(bigtableMock.getMockedDataClient()).readRowsAsync(rowsRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }


  @Test
  public void testExecute() throws Exception {
    verifyReadRequest(rowsRead.readRequest());

    final ResultScanner<Row> resultScanner = Mockito.mock(ResultScanner.class);

    when(bigtableMock.getMockedDataClient().readRows(any())).thenReturn(resultScanner);

    assertEquals(resultScanner, rowsRead.execute());

    verifyReadRequest(rowsRead.readRequest()); // make sure execute did not change the read request
    verify(bigtableMock.getMockedDataClient()).readRows(rowsRead.readRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testLimit() throws Exception {
    final ReadRows.RowsReadImpl read = rowsRead.limit(10);

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    assertEquals(10, readRequest.getRowsLimit());
    assertEquals(RowSet.getDefaultInstance(), readRequest.getRows());
  }

  @Test
  public void testRowRangeOpen() throws Exception {
    final ReadRows.RowsReadImpl rows =
        this.rowsRead.addRowRangeOpen(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("z"));

    final ReadRowsRequest.Builder readRequest = rows.readRequest();
    verifyReadRequest(readRequest);
    final RowSet rowSet = readRequest.getRows();
    assertEquals(0, rowSet.getRowKeysCount());
    assertEquals(1, rowSet.getRowRangesCount());
    assertEquals("a", rowSet.getRowRanges(0).getStartKeyOpen().toStringUtf8());
    assertEquals("z", rowSet.getRowRanges(0).getEndKeyOpen().toStringUtf8());
    assertEquals(ByteString.EMPTY, rowSet.getRowRanges(0).getStartKeyClosed());
    assertEquals(ByteString.EMPTY, rowSet.getRowRanges(0).getEndKeyClosed());
    assertEquals(0, readRequest.getRowsLimit());
    assertEquals(bigtableMock.getMockedDataClient(), rows.getClient());
  }

  @Test
  public void testRowRangeClosed() throws Exception {
    final ReadRows.RowsReadImpl rows =
        this.rowsRead.addRowRangeClosed(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("z"));

    final ReadRowsRequest.Builder readRequest = rows.readRequest();
    verifyReadRequest(readRequest);
    final RowSet rowSet = readRequest.getRows();
    assertEquals(0, rowSet.getRowKeysCount());
    assertEquals(1, rowSet.getRowRangesCount());
    assertEquals("a", rowSet.getRowRanges(0).getStartKeyClosed().toStringUtf8());
    assertEquals("z", rowSet.getRowRanges(0).getEndKeyClosed().toStringUtf8());
    assertEquals(ByteString.EMPTY, rowSet.getRowRanges(0).getStartKeyOpen());
    assertEquals(ByteString.EMPTY, rowSet.getRowRanges(0).getEndKeyOpen());
    assertEquals(0, readRequest.getRowsLimit());
    assertEquals(bigtableMock.getMockedDataClient(), rows.getClient());
  }
}
