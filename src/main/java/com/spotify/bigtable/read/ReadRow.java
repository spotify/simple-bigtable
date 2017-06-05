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

import com.google.bigtable.v2.Row;
import com.spotify.bigtable.read.ReadColumn.ColumnWithinFamilyRead;
import com.spotify.bigtable.read.ReadColumn.ColumnWithinRowsRead;
import com.spotify.bigtable.read.ReadFamilies.FamiliesWithinRowRead;
import com.spotify.bigtable.read.ReadFamilies.FamiliesWithinRowsRead;
import com.spotify.bigtable.read.ReadFamily.FamilyWithinRowRead;
import com.spotify.bigtable.read.ReadFamily.FamilyWithinRowsRead;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;


public class ReadRow {

  interface RowRead<OneFamT, MultiFamT, OneColT, R> extends BigtableRead<R> {

    OneFamT family(final String family);

    MultiFamT familyRegex(final String familyRegex);

    MultiFamT families(final Collection<String> families);

    OneColT column(final String column);
  }

  public interface RowSingleRead extends RowRead<
      FamilyWithinRowRead, FamiliesWithinRowRead, ColumnWithinFamilyRead, Optional<Row>> {
    class ReadImpl
        extends AbstractRowRead<
        FamilyWithinRowRead,
        FamiliesWithinRowRead,
        ColumnWithinFamilyRead,
        Optional<Row>,
        List<Row>>
        implements RowSingleRead {

      ReadImpl(final Internal<List<Row>> parentRead) {
        super(parentRead);
      }

      @Override
      protected Function<List<Row>, Optional<Row>> parentTypeToCurrentType() {
        return AbstractBigtableRead::headOption;
      }

      @Override
      public FamilyWithinRowRead family(final String family) {
        return new FamilyWithinRowRead.ReadImpl(this).columnFamily(family);
      }

      @Override
      public FamiliesWithinRowRead familyRegex(final String familyRegex) {
        return new FamiliesWithinRowRead.ReadImpl(this).familyRegex(familyRegex);
      }

      @Override
      public ColumnWithinFamilyRead column(String column) {
        final List<String> parts = Arrays.asList(column.split(":", 2));
        return family(parts.get(0)).columnQualifier(parts.get(1));
      }
    }
  }

  public interface RowMultiRead extends RowRead<
      FamilyWithinRowsRead, FamiliesWithinRowsRead, ColumnWithinRowsRead, List<Row>> {
    class ReadImpl
        extends AbstractRowRead<
        FamilyWithinRowsRead,
        FamiliesWithinRowsRead,
        ColumnWithinRowsRead,
        List<Row>,
        List<Row>>
        implements RowMultiRead {

      ReadImpl(final Internal<List<Row>> parentRead) {
        super(parentRead);
      }

      @Override
      protected Function<List<Row>, List<Row>> parentTypeToCurrentType() {
        return Function.identity();
      }

      @Override
      public FamilyWithinRowsRead family(final String family) {
        return new FamilyWithinRowsRead.ReadImpl(this).columnFamily(family);
      }

      @Override
      public FamiliesWithinRowsRead familyRegex(String familyRegex) {
        return new FamiliesWithinRowsRead.ReadImpl(this).familyRegex(familyRegex);
      }

      @Override
      public ColumnWithinRowsRead column(String column) {
        final List<String> parts = Arrays.asList(column.split(":", 2));
        return family(parts.get(0)).columnQualifier(parts.get(1));
      }
    }
  }

  private abstract static class AbstractRowRead<OneFamT, MultiFamT, OneColT, R, P>
      extends AbstractBigtableRead<P, R> implements RowRead<OneFamT, MultiFamT, OneColT, R> {

    public AbstractRowRead(Internal<P> parentRead) {
      super(parentRead);
    }

    @Override
    public MultiFamT families(Collection<String> families) {
      return familyRegex(toExactMatchAnyRegex(families));
    }
  }
}
