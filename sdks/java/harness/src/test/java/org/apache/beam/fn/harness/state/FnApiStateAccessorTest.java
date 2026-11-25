/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.fn.harness.state;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FnApiStateAccessor}. */
@RunWith(JUnit4.class)
public class FnApiStateAccessorTest {
  private static final String PTRANSFORM_ID = "ptransform-1";

  private static class ConcatCombineFn extends CombineFn<String, String, String> {
    @Override
    public String createAccumulator() {
      return "";
    }

    @Override
    public String addInput(String accumulator, String input) {
      return accumulator + input;
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumulators) {
      StringBuilder builder = new StringBuilder();
      for (String acc : accumulators) {
        builder.append(acc);
      }
      return builder.toString();
    }

    @Override
    public String extractOutput(String accumulator) {
      return accumulator;
    }
  }

  private static class CountingCoder<T> extends StructuredCoder<T> {
    private final Coder<T> delegate;
    private final boolean overrideConsistentWithEquals;
    private final boolean consistentWithEqualsValue;
    private final AtomicInteger encodeCount = new AtomicInteger();

    CountingCoder(Coder<T> delegate) {
      this(delegate, false, false);
    }

    CountingCoder(Coder<T> delegate, boolean consistentWithEquals) {
      this(delegate, true, consistentWithEquals);
    }

    private CountingCoder(
        Coder<T> delegate,
        boolean overrideConsistentWithEquals,
        boolean consistentWithEqualsValue) {
      this.delegate = delegate;
      this.overrideConsistentWithEquals = overrideConsistentWithEquals;
      this.consistentWithEqualsValue = consistentWithEqualsValue;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
      encodeCount.incrementAndGet();
      delegate.encode(value, outStream);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void encode(T value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      encodeCount.incrementAndGet();
      delegate.encode(value, outStream, context);
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
      return delegate.decode(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      delegate.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
      return overrideConsistentWithEquals
          ? consistentWithEqualsValue
          : delegate.consistentWithEquals();
    }
  }

  private static class NonConsistentByteArrayCoder extends AtomicCoder<byte[]> {
    private final AtomicInteger encodeCount = new AtomicInteger();

    @Override
    public void encode(byte[] value, OutputStream outStream) throws IOException {
      encodeCount.incrementAndGet();
      ByteArrayCoder.of().encode(value, outStream);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void encode(byte[] value, OutputStream outStream, Coder.Context context)
        throws IOException {
      encodeCount.incrementAndGet();
      ByteArrayCoder.of().encode(value, outStream, context);
    }

    @Override
    public byte[] decode(InputStream inStream) throws IOException {
      return ByteArrayCoder.of().decode(inStream);
    }

    @Override
    public boolean consistentWithEquals() {
      return false;
    }
  }

  private static IntervalWindow intervalWindow(long startMs, long endMs) {
    return new IntervalWindow(Instant.ofEpochMilli(startMs), Instant.ofEpochMilli(endMs));
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String freshString(String value) {
    return new String(utf8(value), StandardCharsets.UTF_8);
  }

  private static <T> ByteString encodeValue(Coder<T> coder, T value) throws IOException {
    try (ByteStringOutputStream out = new ByteStringOutputStream()) {
      coder.encode(value, out);
      return out.toByteString();
    }
  }

  @SuppressWarnings("deprecation")
  private static <T> ByteString encodeNested(Coder<T> coder, @Nullable T value) throws IOException {
    try (ByteStringOutputStream out = new ByteStringOutputStream()) {
      coder.encode(value, out, Coder.Context.NESTED);
      return out.toByteString();
    }
  }

  private static final class SideInputFixture<W extends BoundedWindow> {
    final PCollectionView<String> view;
    final SideInputSpec<W> spec;

    SideInputFixture(PCollectionView<String> view, SideInputSpec<W> spec) {
      this.view = view;
      this.spec = spec;
    }

    StateKey stateKey(ByteString encodedWindow) {
      return StateKey.newBuilder()
          .setIterableSideInput(
              StateKey.IterableSideInput.newBuilder()
                  .setTransformId(PTRANSFORM_ID)
                  .setSideInputId(view.getTagInternal().getId())
                  .setWindow(encodedWindow))
          .build();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <W extends BoundedWindow> SideInputFixture<W> createSingletonSideInputFixture(
      @Nullable WindowFn<Object, W> windowFn, Coder<W> windowCoder) {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> sidePc = pipeline.apply(Create.of("unused"));
    if (windowFn != null) {
      sidePc = sidePc.apply(Window.into(windowFn));
    }
    PCollectionView<String> sideView =
        PCollectionViews.singletonView(
            sidePc,
            TypeDescriptors::strings,
            sidePc.getWindowingStrategy(),
            false,
            null,
            StringUtf8Coder.of());
    SideInputSpec<W> sideInputSpec =
        SideInputSpec.create(
            Materializations.ITERABLE_MATERIALIZATION_URN,
            StringUtf8Coder.of(),
            windowCoder,
            sideView.getViewFn(),
            (WindowMappingFn<W>) sideView.getWindowMappingFn());
    return new SideInputFixture<>(sideView, sideInputSpec);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <K> FnApiStateAccessor<K> createAccessor(
      Coder<K> keyCoder,
      Coder<? extends BoundedWindow> windowCoder,
      BeamFnStateClient client,
      AtomicReference<String> instructionId,
      AtomicReference<List<CacheToken>> cacheTokens,
      Cache<?, ?> bundleCache,
      Cache<?, ?> processWideCache,
      Map<TupleTag<?>, SideInputSpec> sideInputSpecMap,
      AtomicReference<K> currentKey,
      AtomicReference<BoundedWindow> currentWindow) {
    FnApiStateAccessor<K> accessor =
        new FnApiStateAccessor<>(
            Pipeline.create().getOptions(),
            Collections.emptySet(),
            PTRANSFORM_ID,
            instructionId::get,
            cacheTokens::get,
            () -> bundleCache,
            processWideCache,
            sideInputSpecMap,
            client,
            keyCoder,
            (Coder<BoundedWindow>) windowCoder);
    accessor.setKeyAndWindowContext(
        new FnApiStateAccessor.MutatingStateContext<K, BoundedWindow>() {
          @Override
          public K getCurrentKey() {
            return currentKey.get();
          }

          @Override
          public BoundedWindow getCurrentWindow() {
            return currentWindow.get();
          }
        });
    return accessor;
  }

  @Test
  public void testByteArrayKeyAndIntervalWindowStructuralReuseAcrossAllStateTypes()
      throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<String> instructionId = new AtomicReference<>("bundle-1");
    AtomicReference<List<CacheToken>> cacheTokens = new AtomicReference<>(Collections.emptyList());
    AtomicReference<byte[]> currentKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>();

    FnApiStateAccessor<byte[]> accessor =
        createAccessor(
            ByteArrayCoder.of(),
            IntervalWindow.getCoder(),
            fakeClient,
            instructionId,
            cacheTokens,
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            currentKey,
            currentWindow);

    StateSpec<ValueState<String>> valueSpec = StateSpecs.value(StringUtf8Coder.of());
    StateSpec<BagState<String>> bagSpec = StateSpecs.bag(StringUtf8Coder.of());
    StateSpec<SetState<String>> setSpec = StateSpecs.set(StringUtf8Coder.of());
    StateSpec<MapState<String, String>> mapSpec =
        StateSpecs.map(StringUtf8Coder.of(), StringUtf8Coder.of());
    StateSpec<MultimapState<String, String>> multimapSpec =
        StateSpecs.multimap(StringUtf8Coder.of(), StringUtf8Coder.of());
    StateSpec<OrderedListState<String>> orderedListSpec =
        StateSpecs.orderedList(StringUtf8Coder.of());
    StateSpec<CombiningState<String, String, String>> combiningSpec =
        StateSpecs.combining(StringUtf8Coder.of(), new ConcatCombineFn());

    // First element in (k1, w1)
    currentKey.set(utf8("key-1"));
    currentWindow.set(intervalWindow(0, 1000));

    ValueState<String> v1 = accessor.bindValue("val", valueSpec, StringUtf8Coder.of());
    BagState<String> b1 = accessor.bindBag("bag", bagSpec, StringUtf8Coder.of());
    SetState<String> s1 = accessor.bindSet("set", setSpec, StringUtf8Coder.of());
    MapState<String, String> m1 =
        accessor.bindMap("map", mapSpec, StringUtf8Coder.of(), StringUtf8Coder.of());
    MultimapState<String, String> mm1 =
        accessor.bindMultimap("mmap", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of());
    OrderedListState<String> ol1 =
        accessor.bindOrderedList("olist", orderedListSpec, StringUtf8Coder.of());
    CombiningState<String, String, String> c1 =
        accessor.bindCombining("comb", combiningSpec, StringUtf8Coder.of(), new ConcatCombineFn());

    v1.write("v1-a");
    b1.add("b1-a");
    s1.add("s1-a");
    m1.put("mk1", "mv1");
    mm1.put("mmk1", "mmv1");
    ol1.add(TimestampedValue.of("ol1-a", Instant.ofEpochMilli(10)));
    c1.add("c1-a");

    // Second element with fresh byte[] and fresh IntervalWindow equal to (k1, w1)
    currentKey.set(utf8("key-1"));
    currentWindow.set(intervalWindow(0, 1000));

    ValueState<String> v1Copy = accessor.bindValue("val", valueSpec, StringUtf8Coder.of());
    BagState<String> b1Copy = accessor.bindBag("bag", bagSpec, StringUtf8Coder.of());
    SetState<String> s1Copy = accessor.bindSet("set", setSpec, StringUtf8Coder.of());
    MapState<String, String> m1Copy =
        accessor.bindMap("map", mapSpec, StringUtf8Coder.of(), StringUtf8Coder.of());
    MultimapState<String, String> mm1Copy =
        accessor.bindMultimap("mmap", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of());
    OrderedListState<String> ol1Copy =
        accessor.bindOrderedList("olist", orderedListSpec, StringUtf8Coder.of());
    CombiningState<String, String, String> c1Copy =
        accessor.bindCombining("comb", combiningSpec, StringUtf8Coder.of(), new ConcatCombineFn());

    assertSame(v1, v1Copy);
    assertSame(b1, b1Copy);
    assertSame(s1, s1Copy);
    assertSame(m1, m1Copy);
    assertSame(mm1, mm1Copy);
    assertSame(ol1, ol1Copy);
    assertSame(c1, c1Copy);

    assertEquals("v1-a", v1Copy.read());
    assertTrue(s1Copy.contains("s1-a").read());
    assertEquals("mv1", m1Copy.get("mk1").read());
    b1Copy.add("b1-b");
    c1Copy.add("+c1-b");

    // Distinct key k2 in same window w1 must be isolated
    currentKey.set(utf8("key-2"));
    currentWindow.set(intervalWindow(0, 1000));

    ValueState<String> v2 = accessor.bindValue("val", valueSpec, StringUtf8Coder.of());
    BagState<String> b2 = accessor.bindBag("bag", bagSpec, StringUtf8Coder.of());
    SetState<String> s2 = accessor.bindSet("set", setSpec, StringUtf8Coder.of());
    MapState<String, String> m2 =
        accessor.bindMap("map", mapSpec, StringUtf8Coder.of(), StringUtf8Coder.of());
    MultimapState<String, String> mm2 =
        accessor.bindMultimap("mmap", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of());
    OrderedListState<String> ol2 =
        accessor.bindOrderedList("olist", orderedListSpec, StringUtf8Coder.of());
    CombiningState<String, String, String> c2 =
        accessor.bindCombining("comb", combiningSpec, StringUtf8Coder.of(), new ConcatCombineFn());

    assertNotSame(v1, v2);
    assertNotSame(b1, b2);
    assertNotSame(s1, s2);
    assertNotSame(m1, m2);
    assertNotSame(mm1, mm2);
    assertNotSame(ol1, ol2);
    assertNotSame(c1, c2);
    assertNull(v2.read());
    v2.write("v2-only");

    // Same k1 bytes in distinct window w2 must be isolated
    currentKey.set(utf8("key-1"));
    currentWindow.set(intervalWindow(1000, 2000));

    ValueState<String> v1W2 = accessor.bindValue("val", valueSpec, StringUtf8Coder.of());
    assertNotSame(v1, v1W2);
    assertNull(v1W2.read());
    v1W2.write("v1-w2");

    // Switching back to a fresh copy of (k1, w1) must recover the original state instances
    currentKey.set(utf8("key-1"));
    currentWindow.set(intervalWindow(0, 1000));

    assertSame(v1, accessor.bindValue("val", valueSpec, StringUtf8Coder.of()));
    assertSame(b1, accessor.bindBag("bag", bagSpec, StringUtf8Coder.of()));
    assertSame(s1, accessor.bindSet("set", setSpec, StringUtf8Coder.of()));
    assertSame(m1, accessor.bindMap("map", mapSpec, StringUtf8Coder.of(), StringUtf8Coder.of()));
    assertSame(
        mm1,
        accessor.bindMultimap("mmap", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of()));
    assertSame(ol1, accessor.bindOrderedList("olist", orderedListSpec, StringUtf8Coder.of()));
    assertSame(
        c1,
        accessor.bindCombining("comb", combiningSpec, StringUtf8Coder.of(), new ConcatCombineFn()));

    accessor.finalizeState();

    // Verify all mutations persisted to FakeBeamFnStateClient via a fresh accessor
    AtomicReference<byte[]> readKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> readWindow = new AtomicReference<>();
    FnApiStateAccessor<byte[]> reader =
        createAccessor(
            ByteArrayCoder.of(),
            IntervalWindow.getCoder(),
            fakeClient,
            new AtomicReference<>("bundle-2"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            readKey,
            readWindow);

    readKey.set(utf8("key-1"));
    readWindow.set(intervalWindow(0, 1000));
    assertEquals("v1-a", reader.bindValue("val", valueSpec, StringUtf8Coder.of()).read());
    assertThat(
        reader.bindBag("bag", bagSpec, StringUtf8Coder.of()).read(),
        containsInAnyOrder("b1-a", "b1-b"));
    ByteString encodedKey1;
    try (ByteStringOutputStream out = new ByteStringOutputStream()) {
      ByteArrayCoder.of().encode(utf8("key-1"), out, Coder.Context.NESTED);
      encodedKey1 = out.toByteString();
    }
    ByteString encodedWindow1;
    try (ByteStringOutputStream out = new ByteStringOutputStream()) {
      IntervalWindow.getCoder().encode(intervalWindow(0, 1000), out);
      encodedWindow1 = out.toByteString();
    }
    ByteString encodedSetElement;
    try (ByteStringOutputStream out = new ByteStringOutputStream()) {
      StringUtf8Coder.of().encode("s1-a", out);
      encodedSetElement = out.toByteString();
    }
    StateKey expectedSetStateKey =
        StateKey.newBuilder()
            .setMultimapUserState(
                StateKey.MultimapUserState.newBuilder()
                    .setTransformId(PTRANSFORM_ID)
                    .setUserStateId("set")
                    .setWindow(encodedWindow1)
                    .setKey(encodedKey1)
                    .setMapKey(encodedSetElement))
            .build();
    assertTrue(fakeClient.getData().containsKey(expectedSetStateKey));
    assertEquals(
        "mv1",
        reader
            .bindMap("map", mapSpec, StringUtf8Coder.of(), StringUtf8Coder.of())
            .get("mk1")
            .read());
    assertThat(
        reader
            .bindMultimap("mmap", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of())
            .get("mmk1")
            .read(),
        containsInAnyOrder("mmv1"));
    assertEquals(
        singletonList(TimestampedValue.of("ol1-a", Instant.ofEpochMilli(10))),
        asList(
            Iterables.toArray(
                reader.bindOrderedList("olist", orderedListSpec, StringUtf8Coder.of()).read(),
                TimestampedValue.class)));
    assertEquals(
        "c1-a+c1-b",
        reader
            .bindCombining("comb", combiningSpec, StringUtf8Coder.of(), new ConcatCombineFn())
            .read());

    readKey.set(utf8("key-2"));
    readWindow.set(intervalWindow(0, 1000));
    assertEquals("v2-only", reader.bindValue("val", valueSpec, StringUtf8Coder.of()).read());

    readKey.set(utf8("key-1"));
    readWindow.set(intervalWindow(1000, 2000));
    assertEquals("v1-w2", reader.bindValue("val", valueSpec, StringUtf8Coder.of()).read());
  }

  @Test
  public void testConsistentWithEqualsCodersEncodeOncePerDistinctKeyAndWindow() {
    CountingCoder<String> countingKeyCoder = new CountingCoder<>(StringUtf8Coder.of());
    CountingCoder<IntervalWindow> countingWindowCoder =
        new CountingCoder<>(IntervalWindow.getCoder());
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<String> currentKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>();

    FnApiStateAccessor<String> accessor =
        createAccessor(
            countingKeyCoder,
            countingWindowCoder,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            currentKey,
            currentWindow);

    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    currentKey.set(freshString("key-A"));
    currentWindow.set(intervalWindow(0, 10));
    ValueState<String> s1A = accessor.bindValue("s1", spec, StringUtf8Coder.of());
    ValueState<String> s2A = accessor.bindValue("s2", spec, StringUtf8Coder.of());
    ValueState<String> s3A = accessor.bindValue("s3", spec, StringUtf8Coder.of());

    // L1 hit: fresh key/window instances equal to the immediately preceding key/window
    currentKey.set(freshString("key-A"));
    currentWindow.set(intervalWindow(0, 10));
    assertSame(s1A, accessor.bindValue("s1", spec, StringUtf8Coder.of()));

    currentKey.set(freshString("key-B"));
    currentWindow.set(intervalWindow(10, 20));
    ValueState<String> s1B = accessor.bindValue("s1", spec, StringUtf8Coder.of());
    ValueState<String> s2B = accessor.bindValue("s2", spec, StringUtf8Coder.of());

    // Interleave back to fresh instances of key-A / w1 and key-B / w2
    currentKey.set(freshString("key-A"));
    currentWindow.set(intervalWindow(0, 10));
    assertSame(s1A, accessor.bindValue("s1", spec, StringUtf8Coder.of()));
    assertSame(s2A, accessor.bindValue("s2", spec, StringUtf8Coder.of()));
    assertSame(s3A, accessor.bindValue("s3", spec, StringUtf8Coder.of()));

    currentKey.set(freshString("key-B"));
    currentWindow.set(intervalWindow(10, 20));
    assertSame(s1B, accessor.bindValue("s1", spec, StringUtf8Coder.of()));
    assertSame(s2B, accessor.bindValue("s2", spec, StringUtf8Coder.of()));

    assertEquals(2, countingKeyCoder.encodeCount.get());
    assertEquals(2, countingWindowCoder.encodeCount.get());
  }

  @Test
  public void testNonConsistentWithEqualsCoderMaintainsStateConsistency() {
    NonConsistentByteArrayCoder customKeyCoder = new NonConsistentByteArrayCoder();
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<byte[]> currentKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);

    FnApiStateAccessor<byte[]> accessor =
        createAccessor(
            customKeyCoder,
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            currentKey,
            currentWindow);

    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    // Same reference across multiple state IDs must encode only once
    byte[] k1 = utf8("key-1");
    currentKey.set(k1);
    ValueState<String> s1K1 = accessor.bindValue("s1", spec, StringUtf8Coder.of());
    ValueState<String> s2K1 = accessor.bindValue("s2", spec, StringUtf8Coder.of());
    assertEquals(1, customKeyCoder.encodeCount.get());
    s1K1.write("first");
    s2K1.write("s2-val");

    // Distinct byte[] reference with identical encoded bytes must still resolve to same ValueState
    currentKey.set(utf8("key-1"));
    ValueState<String> s1K1Copy = accessor.bindValue("s1", spec, StringUtf8Coder.of());
    assertSame(s1K1, s1K1Copy);
    assertEquals("first", s1K1Copy.read());
    s1K1Copy.write("second");

    // Distinct bytes produce an isolated ValueState
    currentKey.set(utf8("key-2"));
    ValueState<String> s1K2 = accessor.bindValue("s1", spec, StringUtf8Coder.of());
    assertNotSame(s1K1, s1K2);
    assertNull(s1K2.read());
    s1K2.write("k2-val");

    // Switching back to yet another fresh byte[] copy of key-1 still returns s1K1
    currentKey.set(utf8("key-1"));
    ValueState<String> s1K1Third = accessor.bindValue("s1", spec, StringUtf8Coder.of());
    assertSame(s1K1, s1K1Third);
    assertEquals("second", s1K1Third.read());

    accessor.finalizeState();

    // Verify persisted bytes in FakeBeamFnStateClient
    AtomicReference<byte[]> readKey = new AtomicReference<>();
    FnApiStateAccessor<byte[]> reader =
        createAccessor(
            new NonConsistentByteArrayCoder(),
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-2"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            readKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));
    readKey.set(utf8("key-1"));
    assertEquals("second", reader.bindValue("s1", spec, StringUtf8Coder.of()).read());
    assertEquals("s2-val", reader.bindValue("s2", spec, StringUtf8Coder.of()).read());
    readKey.set(utf8("key-2"));
    assertEquals("k2-val", reader.bindValue("s1", spec, StringUtf8Coder.of()).read());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testBindCombiningWithContextCachingAndSideInputAccess() throws Exception {
    SideInputFixture<IntervalWindow> sideInput =
        createSingletonSideInputFixture(
            FixedWindows.of(Duration.millis(50)), IntervalWindow.getCoder());

    ByteString encodedWindow1 = encodeValue(IntervalWindow.getCoder(), intervalWindow(50, 100));
    ByteString encodedWindow2 = encodeValue(IntervalWindow.getCoder(), intervalWindow(100, 150));

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(
                sideInput.stateKey(encodedWindow1), singletonList("ctx50"),
                sideInput.stateKey(encodedWindow2), singletonList("ctx100")));

    CombineFnWithContext<String, String, String> combineFnWithContext =
        new CombineFnWithContext<String, String, String>() {
          @Override
          public String createAccumulator(CombineWithContext.Context c) {
            return "";
          }

          @Override
          public String addInput(String accumulator, String input, CombineWithContext.Context c) {
            String prefix = c.sideInput(sideInput.view);
            return accumulator.isEmpty() ? prefix + ":" + input : accumulator + "," + input;
          }

          @Override
          public String mergeAccumulators(
              Iterable<String> accumulators, CombineWithContext.Context c) {
            return String.join(",", accumulators);
          }

          @Override
          public String extractOutput(String accumulator, CombineWithContext.Context c) {
            return c.sideInput(sideInput.view) + "|" + accumulator;
          }
        };

    AtomicReference<String> currentKey = new AtomicReference<>(freshString("k1"));
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(intervalWindow(50, 100));
    FnApiStateAccessor<String> accessor =
        createAccessor(
            StringUtf8Coder.of(),
            IntervalWindow.getCoder(),
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            ImmutableMap.of(sideInput.view.getTagInternal(), sideInput.spec),
            currentKey,
            currentWindow);

    StateSpec<CombiningState<String, String, String>> spec =
        StateSpecs.combining(StringUtf8Coder.of(), combineFnWithContext);

    CombiningState<String, String, String> comb1 =
        accessor.bindCombiningWithContext(
            "combCtx", spec, StringUtf8Coder.of(), combineFnWithContext);
    comb1.add("a");

    // Fresh key/window instances for the same logical (k1, [50, 100)) must return the cached state
    currentKey.set(freshString("k1"));
    currentWindow.set(intervalWindow(50, 100));
    CombiningState<String, String, String> comb1Copy =
        accessor.bindCombiningWithContext(
            "combCtx", spec, StringUtf8Coder.of(), combineFnWithContext);
    assertSame(comb1, comb1Copy);
    comb1Copy.add("b");
    assertEquals("ctx50|ctx50:a,b", comb1Copy.read());

    // Distinct window [100, 150) resolves its own CombiningState and side input window
    currentWindow.set(intervalWindow(100, 150));
    CombiningState<String, String, String> combW2 =
        accessor.bindCombiningWithContext(
            "combCtx", spec, StringUtf8Coder.of(), combineFnWithContext);
    assertNotSame(comb1, combW2);
    combW2.add("z");
    assertEquals("ctx100|ctx100:z", combW2.read());

    accessor.finalizeState();

    // Verify accumulator was persisted for [50, 100)
    ByteString encodedKey = encodeNested(StringUtf8Coder.of(), "k1");
    StateKey expectedStateKey =
        StateKey.newBuilder()
            .setBagUserState(
                StateKey.BagUserState.newBuilder()
                    .setTransformId(PTRANSFORM_ID)
                    .setUserStateId("combCtx")
                    .setWindow(encodedWindow1)
                    .setKey(encodedKey))
            .build();
    List<String> persisted = new ArrayList<>();
    try (InputStream in = fakeClient.getData().get(expectedStateKey).newInput()) {
      while (in.available() > 0) {
        persisted.add(StringUtf8Coder.of().decode(in));
      }
    }
    assertEquals(singletonList("ctx50:a,b"), persisted);
  }

  @Test
  public void testFinalizeStateResetsPerBundleStateAcrossBundlesWithCacheToken() {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    Cache<?, ?> processWideCache = Caches.eternal();
    AtomicReference<String> instructionId = new AtomicReference<>("bundle-1");
    AtomicReference<List<CacheToken>> cacheTokens =
        new AtomicReference<>(
            singletonList(
                CacheToken.newBuilder()
                    .setUserState(CacheToken.UserState.getDefaultInstance())
                    .setToken(ByteString.copyFromUtf8("token-1"))
                    .build()));
    AtomicReference<byte[]> currentKey = new AtomicReference<>(utf8("k1"));
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);

    FnApiStateAccessor<byte[]> accessor =
        createAccessor(
            ByteArrayCoder.of(),
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            instructionId,
            cacheTokens,
            Caches.noop(),
            processWideCache,
            Collections.emptyMap(),
            currentKey,
            currentWindow);

    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    // Bundle 1
    ValueState<String> bundle1State = accessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNull(bundle1State.read());
    bundle1State.write("from-bundle-1");
    accessor.finalizeState();
    int callsAfterBundle1 = fakeClient.getCallCount();

    // Bundle 2 on the same accessor without re-invoking setKeyAndWindowContext
    instructionId.set("bundle-2");
    currentKey.set(utf8("k1"));
    ValueState<String> bundle2State = accessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(bundle1State, bundle2State);
    assertEquals("from-bundle-1", bundle2State.read());
    // Reading from processWideCache should not issue a StateGetRequest
    assertEquals(callsAfterBundle1, fakeClient.getCallCount());

    bundle2State.write("from-bundle-2");
    accessor.finalizeState();

    // Verify Bundle 2 persisted its update (fresh accessor with new cache token forces state read)
    cacheTokens.set(Collections.emptyList());
    instructionId.set("bundle-3");
    assertEquals("from-bundle-2", accessor.bindValue("v", spec, StringUtf8Coder.of()).read());
  }

  @Test
  public void testUnkeyedContextAllowsSideInputAndRejectsUserState() throws Exception {
    SideInputFixture<GlobalWindow> sideInput =
        createSingletonSideInputFixture(null, GlobalWindow.Coder.INSTANCE);

    ByteString encodedGlobalWindow =
        encodeValue(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(
                sideInput.stateKey(encodedGlobalWindow), singletonList("side-input-value")));

    AtomicReference<Void> currentKey = new AtomicReference<>(null);
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiStateAccessor<Void> accessor =
        createAccessor(
            null,
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            ImmutableMap.of(sideInput.view.getTagInternal(), sideInput.spec),
            currentKey,
            currentWindow);

    assertEquals("side-input-value", accessor.get(sideInput.view, GlobalWindow.INSTANCE));

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                accessor.bindValue(
                    "v", StateSpecs.value(StringUtf8Coder.of()), StringUtf8Coder.of()));
    assertThat(ex.getMessage(), containsString("no key coder available"));
  }

  @Test
  public void testBundleCacheFallbackUsesWeightedUserStateCacheKeys() {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    StateSpec<BagState<String>> bagSpec = StateSpecs.bag(StringUtf8Coder.of());
    StateSpec<MultimapState<String, String>> multimapSpec =
        StateSpecs.multimap(StringUtf8Coder.of(), StringUtf8Coder.of());
    StateSpec<OrderedListState<String>> orderedListSpec =
        StateSpecs.orderedList(StringUtf8Coder.of());

    // Seed initial persisted data in an uncached bundle
    FnApiStateAccessor<String> seeder =
        createAccessor(
            StringUtf8Coder.of(),
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-0"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            new AtomicReference<>("k1"),
            new AtomicReference<>(GlobalWindow.INSTANCE));
    seeder.bindBag("shared", bagSpec, StringUtf8Coder.of()).add("bag-init");
    seeder
        .bindMultimap("shared", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of())
        .put("mk", "mm-init");
    seeder
        .bindOrderedList("shared", orderedListSpec, StringUtf8Coder.of())
        .add(TimestampedValue.of("ol-init", Instant.ofEpochMilli(10)));
    seeder.finalizeState();

    // Exercise real bundleCache fallback across all three UserStateCacheKeyBase subclasses
    Caches.ClearableCache<Object, Object> bundleCache =
        new Caches.ClearableCache<>(Caches.subCache(Caches.eternal(), "bundle-1"));
    AtomicReference<String> keyRefA = new AtomicReference<>("k1");
    AtomicReference<BoundedWindow> windowRefA = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiStateAccessor<String> accessorA =
        createAccessor(
            StringUtf8Coder.of(),
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            bundleCache,
            Caches.noop(),
            Collections.emptyMap(),
            keyRefA,
            windowRefA);

    assertThat(
        accessorA.bindBag("shared", bagSpec, StringUtf8Coder.of()).read(),
        containsInAnyOrder("bag-init"));
    assertThat(
        accessorA
            .bindMultimap("shared", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of())
            .get("mk")
            .read(),
        containsInAnyOrder("mm-init"));
    assertEquals(
        singletonList(TimestampedValue.of("ol-init", Instant.ofEpochMilli(10))),
        asList(
            Iterables.toArray(
                accessorA.bindOrderedList("shared", orderedListSpec, StringUtf8Coder.of()).read(),
                TimestampedValue.class)));

    int callsAfterFirstRead = fakeClient.getCallCount();

    // A second accessor sharing bundleCache within the same bundle should hit cached state
    FnApiStateAccessor<String> accessorB =
        createAccessor(
            StringUtf8Coder.of(),
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            bundleCache,
            Caches.noop(),
            Collections.emptyMap(),
            new AtomicReference<>("k1"),
            new AtomicReference<>(GlobalWindow.INSTANCE));

    assertThat(
        accessorB.bindBag("shared", bagSpec, StringUtf8Coder.of()).read(),
        containsInAnyOrder("bag-init"));
    assertThat(
        accessorB
            .bindMultimap("shared", multimapSpec, StringUtf8Coder.of(), StringUtf8Coder.of())
            .get("mk")
            .read(),
        containsInAnyOrder("mm-init"));
    // BagUserState and MultimapUserState serve cached values from bundleCache without RPCs.
    assertEquals(callsAfterFirstRead, fakeClient.getCallCount());
    // OrderedListUserState still constructs an OrderedListUserStateCacheKey in bundleCache, even
    // though readRange currently uses Caches.noop() internally.
    assertEquals(
        singletonList(TimestampedValue.of("ol-init", Instant.ofEpochMilli(10))),
        asList(
            Iterables.toArray(
                accessorB.bindOrderedList("shared", orderedListSpec, StringUtf8Coder.of()).read(),
                TimestampedValue.class)));

    accessorA.finalizeState();
    accessorB.finalizeState();
    bundleCache.clear();
  }

  @Test
  public void testEncodingBufferResetsAfterPartialWriteExceptionAndLargeKeys() throws Exception {
    Coder<String> partialFailCoder =
        new AtomicCoder<String>() {
          @Override
          public void encode(String value, OutputStream outStream) throws IOException {
            encode(value, outStream, Coder.Context.NESTED);
          }

          @Override
          @SuppressWarnings("deprecation")
          public void encode(String value, OutputStream outStream, Coder.Context context)
              throws IOException {
            if ("fail".equals(value)) {
              outStream.write(utf8("BAD"));
              throw new IOException("injected encode failure");
            }
            StringUtf8Coder.of().encode(value, outStream, context);
          }

          @Override
          public String decode(InputStream inStream) throws IOException {
            return StringUtf8Coder.of().decode(inStream);
          }
        };

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<String> currentKey = new AtomicReference<>("ok-1");
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiStateAccessor<String> accessor =
        createAccessor(
            partialFailCoder,
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            currentKey,
            currentWindow);

    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());
    accessor.bindValue("v", spec, StringUtf8Coder.of()).write("v1");

    currentKey.set("fail");
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> accessor.bindValue("v", spec, StringUtf8Coder.of()));
    assertTrue(ex.getCause() instanceof IOException);

    // Subsequent valid key must not see leftover "BAD" prefix bytes from the failed encode
    currentKey.set("ok-2");
    accessor.bindValue("v", spec, StringUtf8Coder.of()).write("v2");
    accessor.finalizeState();

    ByteString encodedWindow = encodeValue(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    StateKey expectedOk1 =
        StateKey.newBuilder()
            .setBagUserState(
                StateKey.BagUserState.newBuilder()
                    .setTransformId(PTRANSFORM_ID)
                    .setUserStateId("v")
                    .setWindow(encodedWindow)
                    .setKey(encodeNested(StringUtf8Coder.of(), "ok-1")))
            .build();
    StateKey expectedOk2 =
        StateKey.newBuilder()
            .setBagUserState(
                StateKey.BagUserState.newBuilder()
                    .setTransformId(PTRANSFORM_ID)
                    .setUserStateId("v")
                    .setWindow(encodedWindow)
                    .setKey(encodeNested(StringUtf8Coder.of(), "ok-2")))
            .build();
    assertEquals(2, fakeClient.getData().size());
    assertTrue(fakeClient.getData().containsKey(expectedOk1));
    assertTrue(fakeClient.getData().containsKey(expectedOk2));

    // Also verify keys exceeding MAX_RETAINED_BYTES (4096) do not corrupt subsequent small keys
    FakeBeamFnStateClient largeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<byte[]> byteKeyRef = new AtomicReference<>(new byte[8192]);
    byteKeyRef.get()[0] = 42;
    byteKeyRef.get()[8191] = 99;
    FnApiStateAccessor<byte[]> largeAccessor =
        createAccessor(
            ByteArrayCoder.of(),
            GlobalWindow.Coder.INSTANCE,
            largeClient,
            new AtomicReference<>("bundle-large"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            byteKeyRef,
            new AtomicReference<>(GlobalWindow.INSTANCE));
    largeAccessor.bindValue("v", spec, StringUtf8Coder.of()).write("large-val");
    byteKeyRef.set(utf8("tiny"));
    largeAccessor.bindValue("v", spec, StringUtf8Coder.of()).write("tiny-val");
    largeAccessor.finalizeState();

    byte[] largeCopy = new byte[8192];
    largeCopy[0] = 42;
    largeCopy[8191] = 99;
    byteKeyRef.set(largeCopy);
    assertEquals("large-val", largeAccessor.bindValue("v", spec, StringUtf8Coder.of()).read());
    byteKeyRef.set(utf8("tiny"));
    assertEquals("tiny-val", largeAccessor.bindValue("v", spec, StringUtf8Coder.of()).read());
  }

  @Test
  public void testFinalizeStateCleansUpPerBundleCachesWhenFinalizerThrows() throws Exception {
    SideInputFixture<GlobalWindow> sideInput =
        createSingletonSideInputFixture(null, GlobalWindow.Coder.INSTANCE);
    ByteString encodedWindow = encodeValue(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    FakeBeamFnStateClient delegateClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(sideInput.stateKey(encodedWindow), singletonList("si-1")));

    AtomicBoolean failAppend = new AtomicBoolean(true);
    BeamFnStateClient failingClient =
        new BeamFnStateClient() {
          @Override
          public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
            if (requestBuilder.getRequestCase() == StateRequest.RequestCase.APPEND
                && failAppend.get()) {
              throw new IllegalStateException("injected persist error");
            }
            return delegateClient.handle(requestBuilder);
          }
        };

    AtomicReference<String> instructionId = new AtomicReference<>("bundle-1");
    AtomicReference<String> currentKey = new AtomicReference<>("k1");
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiStateAccessor<String> accessor =
        createAccessor(
            StringUtf8Coder.of(),
            GlobalWindow.Coder.INSTANCE,
            failingClient,
            instructionId,
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            ImmutableMap.of(sideInput.view.getTagInternal(), sideInput.spec),
            currentKey,
            currentWindow);

    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());
    ValueState<String> bundle1State = accessor.bindValue("v", spec, StringUtf8Coder.of());
    bundle1State.write("failed-write");
    assertEquals("si-1", accessor.get(sideInput.view, GlobalWindow.INSTANCE));
    int callsDuringBundle1 = delegateClient.getCallCount();

    assertThrows(IllegalStateException.class, accessor::finalizeState);

    // Even after finalizeState throws, per-bundle caches and finalizers must be cleared
    failAppend.set(false);
    instructionId.set("bundle-2");
    ValueState<String> bundle2State = accessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(bundle1State, bundle2State);
    assertNull(bundle2State.read());

    // Side input object cache must also have been cleared on finalizeState failure
    assertEquals("si-1", accessor.get(sideInput.view, GlobalWindow.INSTANCE));
    assertTrue(delegateClient.getCallCount() > callsDuringBundle1);

    bundle2State.write("recovered");
    accessor.finalizeState();

    instructionId.set("bundle-3");
    assertEquals("recovered", accessor.bindValue("v", spec, StringUtf8Coder.of()).read());
  }

  @Test
  public void testNullAndNullableKeyTransitions() {
    // VoidCoder with null keys
    FakeBeamFnStateClient voidClient = new FakeBeamFnStateClient(Collections.emptyMap());
    FnApiStateAccessor<Void> voidAccessor =
        createAccessor(
            VoidCoder.of(),
            GlobalWindow.Coder.INSTANCE,
            voidClient,
            new AtomicReference<>("bundle-void"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            new AtomicReference<>(null),
            new AtomicReference<>(GlobalWindow.INSTANCE));
    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());
    ValueState<String> voidV1 = voidAccessor.bindValue("v", spec, StringUtf8Coder.of());
    ValueState<String> voidV2 = voidAccessor.bindValue("v", spec, StringUtf8Coder.of());
    assertSame(voidV1, voidV2);
    voidV1.write("void-val");
    voidAccessor.finalizeState();
    assertEquals("void-val", voidAccessor.bindValue("v", spec, StringUtf8Coder.of()).read());

    // NullableCoder<String> transitioning null -> "a" -> null -> "b" -> "a"
    CountingCoder<String> nullableStringCoder =
        new CountingCoder<>(NullableCoder.of(StringUtf8Coder.of()));
    FakeBeamFnStateClient strClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<String> strKey = new AtomicReference<>(null);
    FnApiStateAccessor<String> strAccessor =
        createAccessor(
            nullableStringCoder,
            GlobalWindow.Coder.INSTANCE,
            strClient,
            new AtomicReference<>("bundle-nullable-str"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            strKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));

    ValueState<String> nullStrState = strAccessor.bindValue("v", spec, StringUtf8Coder.of());
    nullStrState.write("for-null");

    strKey.set(freshString("a"));
    ValueState<String> aState = strAccessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(nullStrState, aState);
    aState.write("for-a");

    strKey.set(null);
    assertSame(nullStrState, strAccessor.bindValue("v", spec, StringUtf8Coder.of()));

    strKey.set(freshString("b"));
    ValueState<String> bState = strAccessor.bindValue("v", spec, StringUtf8Coder.of());
    bState.write("for-b");

    strKey.set(freshString("a"));
    assertSame(aState, strAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    assertEquals(3, nullableStringCoder.encodeCount.get());

    // NullableCoder<byte[]> transitioning null -> k1 -> null -> k2 -> fresh k1
    FakeBeamFnStateClient bytesClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<byte[]> bytesKey = new AtomicReference<>(null);
    FnApiStateAccessor<byte[]> bytesAccessor =
        createAccessor(
            NullableCoder.of(ByteArrayCoder.of()),
            GlobalWindow.Coder.INSTANCE,
            bytesClient,
            new AtomicReference<>("bundle-nullable-bytes"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            bytesKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));

    ValueState<String> nullBytesState = bytesAccessor.bindValue("v", spec, StringUtf8Coder.of());
    nullBytesState.write("bytes-null");
    bytesKey.set(utf8("k1"));
    ValueState<String> k1BytesState = bytesAccessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(nullBytesState, k1BytesState);
    k1BytesState.write("bytes-k1");
    bytesKey.set(null);
    assertSame(nullBytesState, bytesAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    bytesKey.set(utf8("k2"));
    bytesAccessor.bindValue("v", spec, StringUtf8Coder.of()).write("bytes-k2");
    bytesKey.set(utf8("k1"));
    assertSame(k1BytesState, bytesAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    bytesAccessor.finalizeState();

    bytesKey.set(null);
    assertEquals("bytes-null", bytesAccessor.bindValue("v", spec, StringUtf8Coder.of()).read());
    bytesKey.set(utf8("k1"));
    assertEquals("bytes-k1", bytesAccessor.bindValue("v", spec, StringUtf8Coder.of()).read());
    bytesKey.set(utf8("k2"));
    assertEquals("bytes-k2", bytesAccessor.bindValue("v", spec, StringUtf8Coder.of()).read());
  }

  @Test
  public void testInPlaceByteArrayKeyMutationDoesNotAliasState() {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<byte[]> currentKey = new AtomicReference<>();
    FnApiStateAccessor<byte[]> accessor =
        createAccessor(
            ByteArrayCoder.of(),
            GlobalWindow.Coder.INSTANCE,
            fakeClient,
            new AtomicReference<>("bundle-1"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            currentKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));
    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    // Element 1 binds with "key-a", then user code mutates the array in place to "key-b"
    byte[] reusedKey = utf8("key-a");
    currentKey.set(reusedKey);
    ValueState<String> stateA = accessor.bindValue("v", spec, StringUtf8Coder.of());
    stateA.write("val-a");
    reusedKey[4] = 'b';

    // Reusing the same mutated byte[] reference must resolve to "key-b", not alias "key-a"
    ValueState<String> stateBFromReused = accessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(stateA, stateBFromReused);
    assertNull(stateBFromReused.read());
    stateBFromReused.write("val-b");

    // A fresh byte[] matching mutated "key-b" must hit stateBFromReused
    currentKey.set(utf8("key-b"));
    assertSame(stateBFromReused, accessor.bindValue("v", spec, StringUtf8Coder.of()));

    // A fresh byte[] matching pre-mutation "key-a" must still hit stateA (and populate L2 properly)
    currentKey.set(utf8("key-a"));
    assertSame(stateA, accessor.bindValue("v", spec, StringUtf8Coder.of()));

    // Mutate another array inserted into L2 and ensure L2 bucket/equality stays intact
    byte[] kC = utf8("key-c");
    currentKey.set(kC);
    ValueState<String> stateC = accessor.bindValue("v", spec, StringUtf8Coder.of());
    stateC.write("val-c");
    kC[4] = 'd';
    currentKey.set(utf8("key-d"));
    ValueState<String> stateD = accessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(stateC, stateD);
    assertNull(stateD.read());
    currentKey.set(utf8("key-c"));
    assertSame(stateC, accessor.bindValue("v", spec, StringUtf8Coder.of()));

    accessor.finalizeState();
    currentKey.set(utf8("key-a"));
    assertEquals("val-a", accessor.bindValue("v", spec, StringUtf8Coder.of()).read());
    currentKey.set(utf8("key-b"));
    assertEquals("val-b", accessor.bindValue("v", spec, StringUtf8Coder.of()).read());
    currentKey.set(utf8("key-c"));
    assertEquals("val-c", accessor.bindValue("v", spec, StringUtf8Coder.of()).read());
  }

  @Test
  public void testWrappedAndCompositeByteArrayKeyCoders() {
    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    // 1. LengthPrefixCoder<byte[]>
    FakeBeamFnStateClient lpClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<byte[]> lpKey = new AtomicReference<>();
    FnApiStateAccessor<byte[]> lpAccessor =
        createAccessor(
            LengthPrefixCoder.of(ByteArrayCoder.of()),
            GlobalWindow.Coder.INSTANCE,
            lpClient,
            new AtomicReference<>("bundle-lp"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            lpKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));

    lpKey.set(utf8("lp-1"));
    ValueState<String> lpState1 = lpAccessor.bindValue("v", spec, StringUtf8Coder.of());
    lpState1.write("v-lp-1");
    lpKey.set(utf8("lp-1"));
    assertSame(lpState1, lpAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    lpKey.set(utf8("lp-2"));
    ValueState<String> lpState2 = lpAccessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(lpState1, lpState2);
    lpState2.write("v-lp-2");
    lpKey.set(utf8("lp-1"));
    assertSame(lpState1, lpAccessor.bindValue("v", spec, StringUtf8Coder.of()));

    // Verify MemoizedEncodedSupplier directly avoids re-encoding LengthPrefixCoder<byte[]>
    AtomicInteger lpEncodeCount = new AtomicInteger();
    MemoizedEncodedSupplier<byte[]> lpMemoizer =
        new MemoizedEncodedSupplier<>(
            lpKey::get,
            LengthPrefixCoder.of(ByteArrayCoder.of()),
            bytes -> {
              lpEncodeCount.incrementAndGet();
              try {
                return encodeNested(LengthPrefixCoder.of(ByteArrayCoder.of()), bytes);
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
            });
    lpKey.set(utf8("alpha"));
    ByteString alphaEncoded = lpMemoizer.get();
    lpKey.set(utf8("alpha"));
    assertSame(alphaEncoded, lpMemoizer.get());
    lpKey.set(utf8("beta"));
    ByteString betaEncoded = lpMemoizer.get();
    lpKey.set(utf8("alpha"));
    assertSame(alphaEncoded, lpMemoizer.get());
    lpKey.set(utf8("beta"));
    assertSame(betaEncoded, lpMemoizer.get());
    assertEquals(2, lpEncodeCount.get());

    // 2. LengthPrefixCoder<KvCoder<CountingCoder<String>, ByteArrayCoder>>
    CountingCoder<String> kvStringCounter = new CountingCoder<>(StringUtf8Coder.of());
    FakeBeamFnStateClient kvClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<KV<String, byte[]>> kvKey = new AtomicReference<>();
    FnApiStateAccessor<KV<String, byte[]>> kvAccessor =
        createAccessor(
            LengthPrefixCoder.of(KvCoder.of(kvStringCounter, ByteArrayCoder.of())),
            GlobalWindow.Coder.INSTANCE,
            kvClient,
            new AtomicReference<>("bundle-kv"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            kvKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));

    byte[] kvBytes1 = utf8("b1");
    kvKey.set(KV.of(freshString("k1"), kvBytes1));
    ValueState<String> kvState1 = kvAccessor.bindValue("v", spec, StringUtf8Coder.of());
    kvState1.write("kv-1");
    // In-place mutation of byte[] inside KV must not alias
    kvBytes1[1] = 'X';
    ValueState<String> kvStateMutated = kvAccessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(kvState1, kvStateMutated);
    kvKey.set(KV.of(freshString("k1"), utf8("b1")));
    assertSame(kvState1, kvAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    kvKey.set(KV.of(freshString("k2"), utf8("b2")));
    ValueState<String> kvState2 = kvAccessor.bindValue("v", spec, StringUtf8Coder.of());
    kvKey.set(KV.of(freshString("k1"), utf8("b1")));
    assertSame(kvState1, kvAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    kvKey.set(KV.of(freshString("k2"), utf8("b2")));
    assertSame(kvState2, kvAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    assertEquals(3, kvStringCounter.encodeCount.get());

    // 3. ShardedKey.Coder<CountingCoder<String>>
    CountingCoder<String> shardedCounter = new CountingCoder<>(StringUtf8Coder.of());
    FakeBeamFnStateClient skClient = new FakeBeamFnStateClient(Collections.emptyMap());
    AtomicReference<ShardedKey<String>> skKey = new AtomicReference<>();
    FnApiStateAccessor<ShardedKey<String>> skAccessor =
        createAccessor(
            ShardedKey.Coder.of(shardedCounter),
            GlobalWindow.Coder.INSTANCE,
            skClient,
            new AtomicReference<>("bundle-sk"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            Collections.emptyMap(),
            skKey,
            new AtomicReference<>(GlobalWindow.INSTANCE));

    skKey.set(ShardedKey.of(freshString("u1"), utf8("s1")));
    ValueState<String> skState1 = skAccessor.bindValue("v", spec, StringUtf8Coder.of());
    skKey.set(ShardedKey.of(freshString("u1"), utf8("s1")));
    assertSame(skState1, skAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    skKey.set(ShardedKey.of(freshString("u1"), utf8("s2")));
    ValueState<String> skState2 = skAccessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(skState1, skState2);
    skKey.set(ShardedKey.of(freshString("u1"), utf8("s1")));
    assertSame(skState1, skAccessor.bindValue("v", spec, StringUtf8Coder.of()));
    assertEquals(2, shardedCounter.encodeCount.get());
  }

  @Test
  public void testSideInputWindowCachingAndLargeBundleReset() throws Exception {
    CountingCoder<IntervalWindow> countingWindowCoder =
        new CountingCoder<>(IntervalWindow.getCoder(), true);
    SideInputFixture<IntervalWindow> sideInput =
        createSingletonSideInputFixture(FixedWindows.of(Duration.millis(50)), countingWindowCoder);

    ByteString w1 = encodeValue(IntervalWindow.getCoder(), intervalWindow(50, 100));
    ByteString w2 = encodeValue(IntervalWindow.getCoder(), intervalWindow(100, 150));
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(
                sideInput.stateKey(w1), singletonList("val-50"),
                sideInput.stateKey(w2), singletonList("val-100")),
            64);

    AtomicReference<String> instructionId = new AtomicReference<>("bundle-1");
    AtomicReference<String> currentKey = new AtomicReference<>("k");
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(intervalWindow(50, 100));
    FnApiStateAccessor<String> accessor =
        createAccessor(
            StringUtf8Coder.of(),
            IntervalWindow.getCoder(),
            fakeClient,
            instructionId,
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            ImmutableMap.of(sideInput.view.getTagInternal(), sideInput.spec),
            currentKey,
            currentWindow);

    assertEquals("val-50", accessor.get(sideInput.view, intervalWindow(50, 100)));
    assertEquals("val-50", accessor.get(sideInput.view, intervalWindow(50, 100)));
    assertEquals("val-100", accessor.get(sideInput.view, intervalWindow(100, 150)));
    assertEquals("val-100", accessor.get(sideInput.view, intervalWindow(100, 150)));
    assertEquals("val-50", accessor.get(sideInput.view, intervalWindow(50, 100)));
    assertEquals(2, countingWindowCoder.encodeCount.get());
    assertEquals(2, fakeClient.getCallCount());

    // Verify large bundle (> 64 entries) resets maps cleanly without leaking across bundles
    StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());
    ValueState<String> firstKeyState = null;
    for (int i = 0; i < 100; i++) {
      currentKey.set("key-" + i);
      ValueState<String> s = accessor.bindValue("v", spec, StringUtf8Coder.of());
      if (i == 0) {
        firstKeyState = s;
      }
      s.write("val-" + i);
    }
    accessor.finalizeState();

    instructionId.set("bundle-2");
    currentKey.set("key-0");
    ValueState<String> nextBundleFirstState = accessor.bindValue("v", spec, StringUtf8Coder.of());
    assertNotSame(firstKeyState, nextBundleFirstState);
    assertEquals("val-0", nextBundleFirstState.read());

    // Non-consistentWithEquals side-input window coder also avoids duplicate StateGetRequests
    CountingCoder<IntervalWindow> nonConsistentWindowCoder =
        new CountingCoder<>(IntervalWindow.getCoder(), false);
    SideInputFixture<IntervalWindow> nonConsistentSideInput =
        createSingletonSideInputFixture(
            FixedWindows.of(Duration.millis(50)), nonConsistentWindowCoder);
    FakeBeamFnStateClient ncClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(nonConsistentSideInput.stateKey(w1), singletonList("nc-50")));
    FnApiStateAccessor<String> ncAccessor =
        createAccessor(
            StringUtf8Coder.of(),
            IntervalWindow.getCoder(),
            ncClient,
            new AtomicReference<>("bundle-nc"),
            new AtomicReference<>(Collections.emptyList()),
            Caches.noop(),
            Caches.noop(),
            ImmutableMap.of(
                nonConsistentSideInput.view.getTagInternal(), nonConsistentSideInput.spec),
            new AtomicReference<>("k"),
            new AtomicReference<>(intervalWindow(50, 100)));

    assertEquals("nc-50", ncAccessor.get(nonConsistentSideInput.view, intervalWindow(50, 100)));
    assertEquals("nc-50", ncAccessor.get(nonConsistentSideInput.view, intervalWindow(50, 100)));
    assertEquals(1, ncClient.getCallCount());
    assertFalse(nonConsistentWindowCoder.consistentWithEquals());
  }
}
