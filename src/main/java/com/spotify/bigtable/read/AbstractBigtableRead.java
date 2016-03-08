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
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.FuturesExtra;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class which all Bigtable reads should extend.
 * This class lets the duplicate logic all be in a single place while letting the individual class only
 * deal with the interface they care about.
 *
 * @param <P>  Type of parent read. Example Parent of a Family is a Row which would be Optional of Row
 * @param <T>  Return type of query. Example Optional of Cell, List of Cell, Optional of Row, etc.
 */
public abstract class AbstractBigtableRead<P, T> implements BigtableRead<T>, BigtableRead.Internal<T> {

  protected final BigtableRead.Internal<P> parentRead;
  protected final ReadRowsRequest.Builder readRequest;

  public AbstractBigtableRead(final BigtableRead.Internal<P> parentRead) {
    this.parentRead = parentRead;
    this.readRequest = ReadRowsRequest.newBuilder(parentRead.readRequest().build());
  }

  @Override
  public ReadRowsRequest.Builder readRequest() {
    return readRequest;
  }

  @Override
  public BigtableDataClient getClient() {
    return parentRead.getClient();
  }

  @Override
  public ListenableFuture<T> executeAsync() {
    return FuturesExtra.syncTransform(getClient().readRowsAsync(readRequest().build()), this::toDataType);
  }

  @Override
  public T toDataType(final List<Row> rows) {
    return parentDataTypeToDataType(parentRead.toDataType(rows));
  }

  protected abstract T parentDataTypeToDataType(final P parentDataType);

  /**
   * Add the row filter to the read rows request. Uses a chain.
   *
   * @param rowFilter row filter to add
   */
  protected void addRowFilter(RowFilter.Builder rowFilter) {
    final RowFilter.Chain.Builder chain = readRequest.getFilter().getChain().toBuilder();

    if (chain.getFiltersCount() == 0) {
      // Chain must have at least 2 filters
      chain.addFilters(RowFilter.getDefaultInstance());
    }
    chain.addFilters(rowFilter);
    readRequest.setFilter(RowFilter.newBuilder().setChain(chain));
  }

  protected static String toExactMatchRegex(final String input) {
    // TODO: Need to escape all special chars in input
    return input;
  }

  protected static String toExactMatchAnyRegex(final List<String> inputs) {
    // TODO: Need to escape all special chars in inputs
    return toExactMatchRegex("(" + String.join("|", inputs) + ")");
  }


  protected static <A> Optional<A> headOption(final List<A> list) {
    if (list.size() > 1) {
      final String simpleName = list.get(0).getClass().getSimpleName();
      final String message = String.format("Multiple entities of type %s matched when only 1 expected", simpleName);
      throw new RuntimeException(message);
    }
    return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
  }
}
