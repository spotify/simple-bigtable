# Cloud Bigtable Client
[![Build Status](https://travis-ci.com/spotify/bigtable-client.svg?token=2JxpqAi9pfXo9AiMmByK&branch=master)](https://travis-ci.org/spotify/bigtable-client)

## Overview

[Cloud Bigtable](https://cloud.google.com/bigtable/docs/) is a datastore supported by Google for storing huge amounts
of data and maintaining very low read latency. The main drawback to using Bigtable is that Google does
not currently have an official asynchronous client. Within Spotify we have been using the RPC client which is
a pain to use. This library aims to fix that by making the most common interactions with Bigtable clean and easy
while not preventing you from doing anything you could do with the RPC client.

This is very much a work in progress and is just in the early stages of design and implementation.

To import with maven, add this to your pom:

```xml
<dependency>
    <groupId>com.spotify</groupId>
    <artifactId>bigtable-client</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

This dependency should include everything necessary to connect to Bigtable with no extra
dependencies for Linux and Mac users. For others distributions you may need to look into
[configuring OpenSSL for gRPC authentication](https://github.com/grpc/grpc-java/blob/master/SECURITY.md#openssl-statically-linked-netty-tcnative-boringssl-static).

## Raw RPC Client vs Bigtable Client Comparison

### Using The RPC Client

To give an example of using the base RPC client (which gives the `BigtableSession` object), this is how you would
request a single cell from Bigtable.
```java
String project;
String zone;
String cluster;
BigtableSession session;

String fullTableName = String.format("projects/%s/zones/%s/clusters/%s/tables/%s",
        project,
        zone,
        cluster,
        "my-table");

// Could also use a filter chain, but you can't actually set all the filters within the same RowFilter object
// without a merge or chain of some sort
final RowFilter.Builder filter = RowFilter.newBuilder().setFamilyNameRegexFilter("column-family");
filter.mergeFrom(RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8("column-1")).build());
filter.mergeFrom(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build()); // By default it is 1

final ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder()
        .setTableName(fullTableName)
        .setRowKey(ByteString.copyFromUtf8("my-row"))
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
 
Here is the same query as above using this wrapper.
```java
String project;
String zone;
String cluster;
BigtableSession session;

Bigtable bigtable = new Bigtable(session, project, zone, cluster);
final ListenableFuture<Optional<Cell>> cell = bigtable.read("my-table")
    .row("my-row")
    .column("column-family:column-1") // specify both column family and column qualifier separated by colon
    .latestCell()
    .executeAsync();
```

## Performing Reads

The goal of this client is to make the most tedious and common interactions with Bigtable as painless as possible.
 Therefore reading data is an extremely large focus. Here are some examples of reading data.

Get full column family within row
```java
final ListenableFuture<Optional<Family>> family = bigtable.read("my-table")
    .row("my-row")
    .family("my-family")
    .executeAsync();
```

Get multiple columns within a row (Currently all need to be in the same column family but hopefully that gets fixed)
```java
// Single get columns line
final ListenableFuture<List<Column>> family = bigtable.read("my-table")
    .row("my-row")
    .columns(Lists.newArrayList("my-family:qualifier-1", "my-family:qualifier-2"))
    .executeAsync();

// More explicitly within a single column family
final ListenableFuture<List<Column>> family = bigtable.read("my-table")
    .row("my-row")
    .family("my-family")
    .columnQualifiers(Lists.newArrayList("qualifier-1", "qualifier-2"))
    .executeAsync();
```

Get columns within a single family and within column qualifier range
```java
final ListenableFuture<List<Column>> columns = bigtable.read("my-table")
    .row("my-row")
    .columns()
    .family("column-family")
    .startQualifierInclusive(startBytestring)
    .endQualifierExclusive(endBytestring)
    .executeAsync();
```

Get cells between certain timestamps within a column
```java
final ListenableFuture<List<Cell>> cells = bigtable.read("my-table")
    .row("my-row")
    .column("column-family:column-1") // specify both column family and column qualifier separated by colon
    .cells()
    .startTimestampMicros(someTimestamp)
    .endTimestampMicros(someLatertimestamp)
    .executeAsync();
```

Get the latest cell of a certain value within a column
```java
final ListenableFuture<Optional<Cell>> cells = bigtable.read("my-table")
    .row("my-row")
    .column("column-family:column-1") // specify both column family and column qualifier separated by colon
    .cells()
    .startValueInclusive(myValueByteString)
    .endValueInclusive(myValueByteString)
    .latest()
    .executeAsync();
```

## Other Operations

The client supports other Bigtable operations as well, with hopefully the rest of all possible operations coming
soon.

### Mutations (Writes, Deletions)

Mutations are performed on the row level with many mutations possible within a single call. Mutations include
writing new values as well as deleting a column, column family, or an entire row and all data help in each.

Write a new cell within a column
```java
final ListenableFuture<Empty> mutation = bigtable.mutateRow("my-table", "my-row")
    .write("column-family:column-qualifier", ByteString.copyFromUtf8("value"))
    .executeAync()
```

Perform multiple writes in different columns setting an explicit timestamp on some
```java
final ListenableFuture<Empty> mutation = bigtable.mutateRow("my-table", "my-row")
    .write("column-family:column-qualifier", ByteString.copyFromUtf8("value-1"), timestampMicros)
    .write("column-family", "column-qualifier", ByteString.copyFromUtf8("value-2"))
    .executeAync()
```

Delete a column and then write to the same column
```java
final Empty mutation = bigtable.mutateRow("my-table", "my-row")
    .deleteColumn("column-family:column-qualifier")
    .write("column-family:column-qualifier", ByteString.copyFromUtf8("brand-new-value"))
    .execute()
```

### ReadModifyWrite (Atomically Update or Append To A Column)

ReadModifyWrite is useful for either incrementing the latest cell within a column by a long or appenging bytes
to the value. If the column is empty, is will write a new value. Once again this operation is on the row level
with multiple ReadModifyWrites possible in a single request.

Increment a couple counter columns and append a value to another
```java
bigtable.readModifyWrite("my-table", "my-row")
    .read("request-numbers:number-1")
    .thenIncrementAmount(1L)
    .read("request-numbers:number-2")
    .thenIncrementAmount(5L)
    .read("column-family:values")
    .thenAppendValue(ByteString.copyFromUtf8("new-value"))
    .executeAsync();
```

### SampleRowKeys

Sample some row keys from a table.
```java
final List<SampleRowKeysResponse> sampleRowKeys = bigtable.sampleRowKeys("my-table").execute();
```

### CheckAndMutateRow - NOT YET IMPLEMENTED

Perform a read and a set of mutations depending on whether the read returns data. This is not yet implemented but
here are some ideas on how you might perform this operation.

Have the check specified like a read, then allow mutations to be added.
```java
bigtable.checkAndMutateRow("my-table", "my-row")
    .column("column-family:column-qualifier")
    .cells()
    .endTimestampMicros(timestamp)
    .ifExists()
    .deleteColumn("column-family:column-qualifier")
    .write("column-family:column-qualifier", "had-data")
    .ifDoesNotExist()
    .write("column-family:column-qualifier", "did-not-have-data")
    .executeAsync();
```

Pass in Bigtable protobuf objects, kind of against the purpose of the library but keeps things simple.
```java
bigtable.checkAndMutateRow("my-table", "my-row")
    .rowFilter(someRowFilter)
    .ifExists(someMutation)
    .ifExists(someOtherMutation)
    .ifDoesNotExist(someOtherMutation)
    .executeAsync();
```

### Table and Cluster Admin Operations - NOT YET IMPLEMENTED

It is unclear whether there is a need this wrapper to provide the admin operations, though it would be pretty easy
to include. 

## Open Problems and Questions

- One problem is that currently it is not really possible to do queries for nested lists. For example there really is
 no way to do a ColumnRange within a RowRange or request multiple columns within different column families. 
 Something like `BigtableColumnsWithinFamilies` could be added where it keeps track of needing different column
 families but that is confusing. Another option is adding the filtering methods to every Read object which would also
 be super confusing.
- Columns are confusing since a single column is really just a column family and a column qualifier yet they are
 completely separate objects (Family and Column) in the response and add this 4th dimension. I do not really care
 about that family object, it would be much nicer if the Column object also contained the family the column belongs
 to. Perhaps a custom Column object can be made.
- Pass in a metrics registry and get some metrics for free on how long your requests to bigtable are taking.
