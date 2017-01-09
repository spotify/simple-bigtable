package com.spotify.bigtable;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrapper class to convert the repackaged ListenableFuture interface returned by the async calls into the regular
 * version expected by FuturesExtra.
 */
public class ShadedListenableFutureWrapper<V> implements ListenableFuture<V> {

  private final com.google.bigtable.repackaged.com.google.common.util.concurrent.ListenableFuture<V> wrappedInstance;

  public ShadedListenableFutureWrapper(
      final com.google.bigtable.repackaged.com.google.common.util.concurrent.ListenableFuture<V> wrappedInstance) {
    this.wrappedInstance = wrappedInstance;
  }

  @Override
  public void addListener(final Runnable runnable, final Executor executor) {
    wrappedInstance.addListener(runnable, executor);
  }

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return wrappedInstance.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return wrappedInstance.isCancelled();
  }

  @Override
  public boolean isDone() {
    return wrappedInstance.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return wrappedInstance.get();
  }

  @Override
  public V get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return wrappedInstance.get(timeout, unit);
  }
}
