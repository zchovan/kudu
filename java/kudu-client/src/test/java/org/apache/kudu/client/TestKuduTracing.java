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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

/**
 * Verifies the OpenTelemetry instrumentation of the client connection builder and
 * session establishment. No mini-cluster is required: {@code build()} does not
 * connect and {@code newSession()} is a purely local operation, so the spans are
 * produced without a live Kudu server.
 */
public class TestKuduTracing {

  private static final String MASTERS = "127.0.0.1:7051";

  @Rule
  public RetryRule retryRule = new RetryRule();

  private InMemorySpanExporter exporter;
  private OpenTelemetrySdk sdk;

  @Before
  public void setUp() {
    GlobalOpenTelemetry.resetForTest();
    exporter = InMemorySpanExporter.create();
    sdk = OpenTelemetrySdk.builder()
        .setTracerProvider(SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build())
        .buildAndRegisterGlobal();
  }

  @After
  public void tearDown() {
    if (sdk != null) {
      sdk.close();
    }
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  public void testConnectAndSessionSpansEmitted() throws Exception {
    // build() must not block or connect; newSession() is fully local.
    try (KuduClient client = new KuduClient.KuduClientBuilder(MASTERS).build()) {
      client.newSession();
    }

    List<SpanData> spans = exporter.getFinishedSpanItems();
    List<String> names = spans.stream().map(SpanData::getName).collect(Collectors.toList());
    assertTrue("expected a kudu.client.connect span, got " + names,
        names.contains("kudu.client.connect"));
    assertTrue("expected a kudu.session.open span, got " + names,
        names.contains("kudu.session.open"));

    SpanData connect = spans.stream()
        .filter(s -> s.getName().equals("kudu.client.connect"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no connect span"));
    assertEquals(MASTERS,
        connect.getAttributes().get(AttributeKey.stringKey("kudu.master_addresses")));
    assertEquals(Long.valueOf(1L),
        connect.getAttributes().get(AttributeKey.longKey("kudu.num_masters")));
    // Instrumentation scope is set from KuduTracing.INSTRUMENTATION_SCOPE.
    assertEquals("org.apache.kudu.client", connect.getInstrumentationScopeInfo().getName());
  }

  @Test
  public void testTracerIsAlwaysAvailable() {
    // Even with no SDK the API resolves to a no-op tracer, never null.
    assertNotNull(GlobalOpenTelemetry.getTracer("org.apache.kudu.client"));
  }
}
