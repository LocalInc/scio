/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.transforms;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.DoFn;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * A base {@link DoFn} that handles asynchronous requests to an external service.
 */
public abstract class AsyncDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
  /**
   * Set up external resources, like clients to external services.
   */
  // FIXME: this might set up one copy per instance (core)
  public abstract void setupResources();

  /**
   * Process an element asynchronously.
   */
  public abstract ListenableFuture<OutputT> processElement(InputT input);

  private ConcurrentMap<ListenableFuture<OutputT>, Boolean> futures;
  private ConcurrentLinkedQueue<OutputT> results;
  private ConcurrentLinkedQueue<Throwable> errors;

  @Setup
  public void setup() {
    setupResources();
  }

  @StartBundle
  public void startBundle(Context c) {
    futures = Maps.newConcurrentMap();
    results = Queues.newConcurrentLinkedQueue();
    errors = Queues.newConcurrentLinkedQueue();
  }

  @FinishBundle
  public void finishBundle(Context c) {
    if (!futures.isEmpty()) {
      try {
        Futures.allAsList(futures.keySet()).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Failed to process futures", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Failed to process futures", e);
      }
    }

    flush(c);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    flush(c);

    ListenableFuture<OutputT> f = processElement(c.element());
    futures.put(f, false);

    Futures.addCallback(f, new FutureCallback<OutputT>() {
      @Override
      public void onSuccess(@Nullable OutputT result) {
        results.add(result);
        futures.remove(f);
      }

      @Override
      public void onFailure(Throwable t) {
        errors.add(t);
        futures.remove(f);
      }
    });
  }

  private void flush(Context c) {
    if (!errors.isEmpty()) {
      throw new RuntimeException("Error processing elements", errors.element());
    }
    results.forEach(c::output);
    results.clear();
    errors.clear();
  }

}
