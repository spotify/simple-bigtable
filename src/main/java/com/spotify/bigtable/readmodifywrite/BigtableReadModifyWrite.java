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

package com.spotify.bigtable.readmodifywrite;

import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;

public interface BigtableReadModifyWrite {

  ReadModifyWriteRowResponse execute();

  ListenableFuture<ReadModifyWriteRowResponse> executeAsync();

  BigtableReadModifyWrite.Read read(final String column);

  BigtableReadModifyWrite.Read read(final String columnFamily, final String columnQualifier);

  interface Read {

    BigtableReadModifyWrite thenAppendValue(final ByteString value);

    BigtableReadModifyWrite thenIncrementAmount(final long incBy);

  }

}
