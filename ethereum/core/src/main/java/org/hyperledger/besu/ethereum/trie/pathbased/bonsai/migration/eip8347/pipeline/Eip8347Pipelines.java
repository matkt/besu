/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;
import org.hyperledger.besu.services.pipeline.exception.AsyncOperationException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Runs EIP-8347 offline pipelines on Besu's {@code services:pipeline}.
 *
 * <p>Pipes between stages are bounded ({@link #BUFFER_SIZE} items), so a fast stage blocks rather
 * than piling items up in heap. Errors from any stage abort the pipeline and are rethrown as-is.
 *
 * <p>Every item costs a few microseconds of hand-off between stages, which dominates when items are
 * as small as one account or one stem. {@link #fromLists} therefore moves items in lists of about
 * {@link #LIST_WEIGHT} entries.
 */
public final class Eip8347Pipelines {

  /** Items buffered between two stages. */
  public static final int BUFFER_SIZE = 256;

  /** Threads for CPU-bound parallel stages (hashing, code checks). */
  public static final int PARALLELISM = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);

  /** Weight (entries, leaves) grouped into one item by {@link #fromLists}. */
  public static final int LIST_WEIGHT = 1024;

  /** Lists buffered between two stages: enough to keep every parallel worker busy. */
  private static final int LIST_BUFFER_SIZE = 4 * PARALLELISM;

  private Eip8347Pipelines() {}

  public static <T> PipelineBuilder<T, T> from(final String name, final Iterator<T> source) {
    return from(name, source, BUFFER_SIZE);
  }

  /**
   * {@code function} as an asynchronous step on the common fork-join pool, for {@code
   * thenProcessAsyncOrdered}: items are processed in parallel and come out in order.
   */
  public static <T, O> Function<T, CompletableFuture<O>> async(final Function<T, O> function) {
    return item -> CompletableFuture.supplyAsync(() -> function.apply(item));
  }

  /**
   * A pipeline over {@code source} in lists: consecutive items are grouped until their total {@code
   * weight} reaches {@link #LIST_WEIGHT} (an item heavier than that gets a list of its own).
   */
  public static <T> PipelineBuilder<List<T>, List<T>> fromLists(
      final String name, final Iterator<T> source, final ToIntFunction<T> weight) {
    return from(name, inLists(source, weight), LIST_BUFFER_SIZE);
  }

  private static <T> Iterator<List<T>> inLists(
      final Iterator<T> source, final ToIntFunction<T> weight) {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public List<T> next() {
        if (!source.hasNext()) {
          throw new NoSuchElementException();
        }
        final List<T> list = new ArrayList<>();
        int total = 0;
        while (source.hasNext() && total < LIST_WEIGHT) {
          final T item = source.next();
          list.add(item);
          total += Math.max(1, weight.applyAsInt(item));
        }
        return list;
      }
    };
  }

  /** Same as {@link #from(String, Iterator)} with {@code bufferSize} items between stages. */
  public static <T> PipelineBuilder<T, T> from(
      final String name, final Iterator<T> source, final int bufferSize) {
    return PipelineBuilder.createPipelineFrom(
        name, source, bufferSize, NoOpMetricsSystem.NO_OP_LABELLED_2_COUNTER, false, name);
  }

  /** Starts {@code pipeline}, waits for it, and rethrows the first stage failure unwrapped. */
  public static void run(final Pipeline<?> pipeline) throws IOException {
    final AtomicInteger threadId = new AtomicInteger();
    final ExecutorService executor =
        Executors.newCachedThreadPool(
            runnable -> {
              final Thread thread =
                  new Thread(runnable, "eip8347-pipeline-" + threadId.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    try {
      pipeline.start(executor).get();
    } catch (final InterruptedException e) {
      pipeline.abort();
      Thread.currentThread().interrupt();
      throw new IOException("EIP-8347 pipeline interrupted", e);
    } catch (final ExecutionException e) {
      throw rethrow(e.getCause());
    } finally {
      executor.shutdownNow();
    }
  }

  /** Waits for {@code future} and rethrows its failure unwrapped, like {@link #run}. */
  public static <T> T await(final CompletableFuture<T> future) throws IOException {
    try {
      return future.join();
    } catch (final CompletionException | CancellationException e) {
      throw rethrow(e);
    }
  }

  private static IOException rethrow(final Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException
            || cause instanceof ExecutionException
            || cause instanceof AsyncOperationException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof UncheckedIOException unchecked) {
      return unchecked.getCause();
    }
    if (cause instanceof IOException io) {
      return io;
    }
    if (cause instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (cause instanceof Error error) {
      throw error;
    }
    return new IOException(cause);
  }
}
