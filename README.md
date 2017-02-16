# Simple Bigtable

## Overview

[Cloud Bigtable](https://cloud.google.com/bigtable/docs/) is a datastore supported by Google for storing huge amounts
of data and maintaining very low read latency. The main drawback to using Bigtable is that Google does
not currently have an official asynchronous client. Within Spotify we have been using the RPC client which is
a pain to use. This library aims to fix that by making the most common interactions with Bigtable clean and easy
to use while not preventing you from doing anything you could do with the RPC client.

This is very much a work in progress and is just in the early stages of design and implementation.

To import with maven, add this to your pom:

```xml
<dependency>
    <groupId>com.spotify</groupId>
    <artifactId>simple-bigtable</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Raw RPC Client vs Bigtable Client Comparison

### Using The RPC Client

To give an example of using the base RPC client (which gives the `BigtableSession` object), this is how you would
request a single cell from Bigtable.
```java
String projectId;
String zone;
String cluster;
BigtableSession session;

String fullTableName = String.format("projects/%s/zones/%s/clusters/%s/tables/%s",
        projectId,
        zone,
        cluster,
        "table");

// Could also use a filter chain, but you can't actually set all the filters within the same RowFilter object
// without a merge or chain of some sort
final RowFilter.Builder filter = RowFilter.newBuilder().setFamilyNameRegexFilter("column-family");
filter.mergeFrom(RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8("column-1")).build());
filter.mergeFrom(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build()); // By default it is 1

final ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder()
        .setTableName(fullTableName)
        .setRowKey(ByteString.copyFromUtf8("row"))
        .setNumRowsLimit(1)
        .setFilter(filter.build())
        .build();

final ListenableFuture<List<Row>> future = session.getDataClient().readRowsAsync(readRowsRequest);
final ListenableFuture<Cell> cell = FuturesExtra.syncTransform(future, rows -> {
  // This doesnt actually check if the row, column family, and qualifier exist
  // IndexOutOfBoundsException might be thrown
  return rows.get(0).getFamilies(0).getColumns(0).getCells(0);
});
```

### Bigtable Client

The goal of this client is to let you query what you want with minimal overhead (there should
be no need to create all these filter objects) as well as give you the object you want without
needing to constantly convert a list of rows down to a single cell.
 
Here is the same query as above using this client wrapper.
```java
String projectId;
String zone;
String cluster;
BigtableSession session;

Bigtable bigtable = new Bigtable(session, projectId, zone, cluster);
final ListenableFuture<Optional<Cell>> cell = bigtable.read("table")
    .row("row")
    .column("family:qualifier") // specify both column family and column qualifier separated by colon
    .latestCell()
    .executeAsync();
```

## Performing Reads

The goal of this client is to make the most tedious and common interactions with Bigtable as painless as possible.
 Therefore reading data is an extremely large focus. Here are some examples of reading data.

Get full column family within row
```java
final ListenableFuture<Optional<Family>> family = bigtable.read("table")
    .row("row")
    .family("family")
    .executeAsync();
```

Get multiple columns within a row (Currently all need to be in the same column family but hopefully that gets fixed)
```java
// Get the entire column
final ListenableFuture<List<Column>> family = bigtable.read("table")
    .row("row")
    .family("family")
    .columnQualifiers(Lists.newArrayList("qualifier-1", "qualifier-2"))
    .executeAsync();

// Get the latest cell in each column
final ListenableFuture<List<Column>> family = bigtable.read("table")
    .row("row")
    .family("family")
    .columnQualifiers(Lists.newArrayList("qualifier1", "qualifier2"))
    .latestCell()
    .executeAsync();
```

Get columns within a single family and within column qualifier range
```java
final ListenableFuture<List<Column>> columns = bigtable.read("table")
    .row("row")
    .family("family")
    .columns()
    .startQualifierInclusive(startBytestring)
    .endQualifierExclusive(endBytestring)
    .executeAsync();
```

Get cells between certain timestamps within a column
```java
final ListenableFuture<List<Cell>> cells = bigtable.read("table")
    .row("row")
    .column("family:qualifier")
    .cells()
    .startTimestampMicros(someTimestamp)
    .endTimestampMicros(someLatertimestamp)
    .executeAsync();
```

Get the latest cell of a certain value within a column
```java
final ListenableFuture<Optional<Cell>> cells = bigtable.read("table")
    .row("row")
    .column("family:qualifier")
    .cells()
    .startValueInclusive(myValueByteString)
    .endValueInclusive(myValueByteString)
    .latest()
    .executeAsync();
```

Get the latest cell of a between 2 timestamps within a column for multiple rows
```java
final ListenableFuture<List<Row>> cells = bigtable.read("table")
    .rows(ImmutableSet.of("row1", "row2"))
    .column("family:qualifier")
    .cells()
    .startTimestampMicros(someTimestamp)
    .endTimestampMicros(someLatertimestamp)
    .latest()
    .executeAsync();
```

Get the multiple column families and column qualifiers (will match all combinations)
```java
final ListenableFuture<List<Row>> cells = bigtable.read("table")
    .row("row")
    .families(ImmutableSet.of("family1, family2"))
    .columnQualifiers(ImmutableSet.of("qualifier1", "qualifier2")
    .cells()
    .startTimestampMicros(someTimestamp)
    .endTimestampMicros(someLatertimestamp)
    .latest()
    .executeAsync();
```

Get all rows between different ranges or with certain specific keys
(these functions add rows to the row set, instead of filtering)
```java
final ListenableFuture<List<Row>> rows = bigtable.read("table")
    .rows()
    .addRowRangeOpen(myStartKeyOpen, myEndKeyOpen) // add an exclusive range
    .addRowRangeClosed(myStartKeyClosed, myEndKeyClosed) // add an inclusive range
    .addKeys(extraKeys) // add some keys you always want
    .executeAsync();
```
Note that currently there is no half open, half closed range.

## Other Operations

The client supports other Bigtable operations as well, with hopefully the rest of all possible operations coming
soon.

### Mutations (Writes, Deletions)

Mutations are performed on the row level with many mutations possible within a single call. Mutations include
writing new values as well as deleting a column, column family, or an entire row and all data help in each.

Write a new cell within a column
```java
final ListenableFuture<Empty> mutation = bigtable.mutateRow("table", "row")
    .write("family:qualifier", ByteString.copyFromUtf8("value"))
    .executeAync()
```

Perform multiple writes in different columns setting an explicit timestamp on some
```java
final ListenableFuture<Empty> mutation = bigtable.mutateRow("table", "row")
    .write("family:qualifier", ByteString.copyFromUtf8("value-1"), timestampMicros)
    .write("family", "qualifier", ByteString.copyFromUtf8("value-2"))
    .executeAync()
```

Delete a column and then write to the same column
```java
final Empty mutation = bigtable.mutateRow("table", "row")
    .deleteColumn("family:qualifier")
    .write("family:qualifier", ByteString.copyFromUtf8("brand-new-value"))
    .execute()
```

### ReadModifyWrite (Atomically Update or Append To A Column)

ReadModifyWrite is useful for either incrementing the latest cell within a column by a long or appenging bytes
to the value. If the column is empty, is will write a new value. Once again this operation is on the row level
with multiple ReadModifyWrites possible in a single request.

Increment a couple counter columns and append a value to another
```java
bigtable.readModifyWrite("table", "row")
    .read("request-numbers:number-1")
    .thenIncrementAmount(1L)
    .read("request-numbers:number-2")
    .thenIncrementAmount(5L)
    .read("family:values")
    .thenAppendValue(ByteString.copyFromUtf8("new-value"))
    .executeAsync();
```

### SampleRowKeys

Sample some row keys from a table.
```java
final List<SampleRowKeysResponse> sampleRowKeys = bigtable.sampleRowKeys("table").execute();
```

### CheckAndMutateRow - NOT YET IMPLEMENTED

Perform a read and a set of mutations depending on whether the read returns data. This is not yet implemented but
here are some ideas on how this operation might be implemented in the future.

Have the check specified like a read, then allow mutations to be added.
```java
bigtable.checkAndMutateRow("table", "row")
    .column("family:qualifier")
    .cells()
    .endTimestampMicros(timestamp)
    .ifExists()
    .deleteColumn("family:qualifier")
    .write("family:qualifier", "had-data")
    .ifDoesNotExist()
    .write("family:qualifier", "did-not-have-data")
    .executeAsync();
```

Pass in Bigtable protobuf objects, kind of against the purpose of the library but keeps things simple.
```java
bigtable.checkAndMutateRow("table", "row")
    .rowFilter(someRowFilter)
    .ifExists(someMutation)
    .ifExists(someOtherMutation)
    .ifDoesNotExist(someOtherMutation)
    .executeAsync();
```
Pull requests with other ideas are encouraged.

### Table and Cluster Admin Operations - NOT YET IMPLEMENTED

It is unclear whether there is a need this wrapper to provide the admin operations, though it would be pretty easy
to include. 

## Open Problems and Questions

- One problem is that currently it is not really possible to do queries for nested lists. For example there really is
 no way to do a ColumnRange within a RowRange or request multiple columns within different column families. 
 Something like `BigtableColumnsWithinFamilies` could be added where it keeps track of needing different column
 families but that is confusing. Another option is adding the filtering methods to every Read object which would also
 be super confusing.

## Code of conduct
This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are expected to honor this code.

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
