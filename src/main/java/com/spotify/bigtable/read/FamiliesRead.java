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
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;

import java.util.List;
import java.util.Optional;

public interface FamiliesRead extends BigtableRead<List<Family>> {

  class FamiliesReadImpl extends AbstractBigtableRead<Optional<Row>, List<Family>> implements FamiliesRead {

    public FamiliesReadImpl(final BigtableRead.Internal<Optional<Row>> row) {
      super(row);
    }

    public FamiliesReadImpl(final BigtableRead.Internal<Optional<Row>> row, final RowFilter.Builder rowFilter) {
      this(row);
      addRowFilter(rowFilter);
    }

    @Override
    protected List<Family> parentDataTypeToDataType(final Optional<Row> row) {
      return row.map(Row::getFamiliesList).orElse(Lists.newArrayList());
    }
  }

}
