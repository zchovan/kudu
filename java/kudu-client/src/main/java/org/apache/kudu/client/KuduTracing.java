// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.client;

import java.util.function.Supplier;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Small helper for emitting OpenTelemetry spans from the Kudu client.
 *
 * <p>The tracer is obtained from {@link GlobalOpenTelemetry}. When the host
 * application has not installed an OpenTelemetry SDK this resolves to a no-op
 * implementation, so instrumentation is effectively free and never throws.
 * When an SDK is configured the client's spans are exported through it, exactly
 * like SLF4J logging flows to whatever binding the application provides.
 */
@InterfaceAudience.Private
final class KuduTracing {

  /** OpenTelemetry instrumentation scope name for all spans emitted by the client. */
  static final String INSTRUMENTATION_SCOPE = "org.apache.kudu.client";

  private KuduTracing() {
  }

  static Tracer tracer() {
    return GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE);
  }

  /**
   * Runs {@code body} inside a span with the given name, making the span current
   * for the duration of the call. The span records any {@link RuntimeException}
   * thrown by {@code body}, marks itself as errored, and is always ended.
   *
   * @param spanName the name of the span
   * @param customizer applied to the {@link Span} before the body runs, e.g. to
   *                   attach attributes; may be null
   * @param body the work to execute within the span
   * @param <T> the result type of {@code body}
   * @return whatever {@code body} returns
   */
  static <T> T inSpan(String spanName, SpanCustomizer customizer, Supplier<T> body) {
    Span span = tracer().spanBuilder(spanName).startSpan();
    if (customizer != null) {
      customizer.customize(span);
    }
    try (Scope scope = span.makeCurrent()) {
      return body.get();
    } catch (RuntimeException e) {
      span.recordException(e);
      span.setStatus(StatusCode.ERROR, e.getMessage() == null ? "" : e.getMessage());
      throw e;
    } finally {
      span.end();
    }
  }

  /** Hook for attaching attributes to a span before the instrumented body runs. */
  @FunctionalInterface
  interface SpanCustomizer {
    void customize(Span span);
  }
}
