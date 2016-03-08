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
import com.google.bigtable.v1.RowRange;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RowsReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  RowsRead.RowsReadImpl rowsRead;

  @Before
  public void setUp() throws Exception {
    final TableRead.TableReadImpl tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
    rowsRead = new RowsRead.RowsReadImpl(tableRead);
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
  }

  @Test
  public void testParentDataTypeToDataType() throws Exception {
    final List<Row> rows = ImmutableList.of(Row.getDefaultInstance());
    assertEquals(rows, rowsRead.parentDataTypeToDataType(rows));
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
    final RowsRead.RowsReadImpl read = (RowsRead.RowsReadImpl) rowsRead.limit(10);

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    assertEquals(10, readRequest.getNumRowsLimit());
  }

  @Test
  public void testAllowRowInterleaving() throws Exception {
    final RowsRead.RowsReadImpl read = (RowsRead.RowsReadImpl) rowsRead.allowRowInterleaving(true);

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    assertTrue(readRequest.getAllowRowInterleaving());
  }

  @Test
  public void testStartKey() throws Exception {
    final RowRange rowRange = RowRange.newBuilder().setStartKey(ByteString.copyFromUtf8("start")).build();
    final RowsRead.RowsReadImpl read = (RowsRead.RowsReadImpl) rowsRead.startKey(rowRange.getStartKey());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    assertEquals(rowRange, readRequest.getRowRange());
  }

  @Test
  public void testEndKey() throws Exception {
    final RowRange rowRange = RowRange.newBuilder().setEndKey(ByteString.copyFromUtf8("end")).build();
    final RowsRead.RowsReadImpl read = (RowsRead.RowsReadImpl) rowsRead.endKey(rowRange.getEndKey());

    final ReadRowsRequest.Builder readRequest = read.readRequest();
    verifyReadRequest(readRequest);

    assertEquals(rowRange, readRequest.getRowRange());
  }
}
