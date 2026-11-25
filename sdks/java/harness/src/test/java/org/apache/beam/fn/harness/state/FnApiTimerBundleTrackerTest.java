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

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.state.FnApiTimerBundleTracker.Modifications;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FnApiTimerBundleTracker}. */
@RunWith(JUnit4.class)
public class FnApiTimerBundleTrackerTest {
  private static class CountingCoder<T> extends StructuredCoder<T> {
    private final Coder<T> delegate;
    private final AtomicInteger encodeCount = new AtomicInteger();

    CountingCoder(Coder<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws IOException {
      encodeCount.incrementAndGet();
      delegate.encode(value, outStream);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void encode(T value, OutputStream outStream, Coder.Context context) throws IOException {
      encodeCount.incrementAndGet();
      delegate.encode(value, outStream, context);
    }

    @Override
    public T decode(InputStream inStream) throws IOException {
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
      return delegate.consistentWithEquals();
    }
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String freshString(String value) {
    return new String(utf8(value), StandardCharsets.UTF_8);
  }

  private static IntervalWindow intervalWindow(long startMs, long endMs) {
    return new IntervalWindow(Instant.ofEpochMilli(startMs), Instant.ofEpochMilli(endMs));
  }

  private static <K> Timer<K> timer(
      K key, String tag, BoundedWindow window, long fireMs, long holdMs) {
    return Timer.of(
        key,
        tag,
        singletonList(window),
        Instant.ofEpochMilli(fireMs),
        Instant.ofEpochMilli(holdMs),
        PaneInfo.NO_FIRING,
        CausedByDrain.NORMAL);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testByteArrayKeyAndCompositeCoderCoalescing() {
    AtomicReference<byte[]> currentKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>();
    FnApiTimerBundleTracker<byte[]> tracker =
        new FnApiTimerBundleTracker<>(
            ByteArrayCoder.of(),
            (Coder<BoundedWindow>) (Coder) IntervalWindow.getCoder(),
            currentKey::get,
            currentWindow::get);

    // Multiple fresh byte[] and IntervalWindow copies for (k1, w1) must share one Modifications
    byte[] k1a = utf8("key-1");
    IntervalWindow w1a = intervalWindow(0, 100);
    currentKey.set(k1a);
    currentWindow.set(w1a);
    tracker.timerModified("t1", TimeDomain.EVENT_TIME, timer(k1a, "tag-a", w1a, 50, 50));
    Modifications<byte[]> k1w1Mods = tracker.getBundleModifications();

    byte[] k1b = utf8("key-1");
    IntervalWindow w1b = intervalWindow(0, 100);
    currentKey.set(k1b);
    currentWindow.set(w1b);
    tracker.timerModified("t1", TimeDomain.EVENT_TIME, timer(k1b, "tag-a", w1b, 60, 60));
    assertSame(k1w1Mods, tracker.getBundleModifications());

    // Distinct key k2 creates a separate Modifications partition
    byte[] k2 = utf8("key-2");
    currentKey.set(k2);
    tracker.timerModified("t1", TimeDomain.PROCESSING_TIME, timer(k2, "tag-b", w1b, 70, 70));
    assertNotSame(k1w1Mods, tracker.getBundleModifications());

    // Returning to a fresh copy of (k1, w1) recovers k1w1Mods
    currentKey.set(utf8("key-1"));
    currentWindow.set(intervalWindow(0, 100));
    assertSame(k1w1Mods, tracker.getBundleModifications());

    List<String> emitted = new ArrayList<>();
    tracker.outputTimers(
        timerId ->
            t -> emitted.add(timerId + ":" + t.getDynamicTimerTag() + "@" + t.getFireTimestamp()));
    assertThat(
        emitted,
        containsInAnyOrder(
            "t1:tag-a@1970-01-01T00:00:00.060Z", "t1:tag-b@1970-01-01T00:00:00.070Z"));

    // Composite LengthPrefixCoder<KvCoder<CountingCoder<String>, ByteArrayCoder>> encodes once per
    // key
    CountingCoder<String> kvStringCounter = new CountingCoder<>(StringUtf8Coder.of());
    CountingCoder<IntervalWindow> windowCounter = new CountingCoder<>(IntervalWindow.getCoder());
    AtomicReference<KV<String, byte[]>> kvKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> kvWindow = new AtomicReference<>();
    FnApiTimerBundleTracker<KV<String, byte[]>> kvTracker =
        new FnApiTimerBundleTracker<>(
            LengthPrefixCoder.of(KvCoder.of(kvStringCounter, ByteArrayCoder.of())),
            (Coder<BoundedWindow>) (Coder) windowCounter,
            kvKey::get,
            kvWindow::get);

    kvKey.set(KV.of(freshString("a"), utf8("1")));
    kvWindow.set(intervalWindow(10, 20));
    Modifications<KV<String, byte[]>> kvMods1 = kvTracker.getBundleModifications();
    kvKey.set(KV.of(freshString("a"), utf8("1")));
    kvWindow.set(intervalWindow(10, 20));
    assertSame(kvMods1, kvTracker.getBundleModifications());

    kvKey.set(KV.of(freshString("b"), utf8("2")));
    kvWindow.set(intervalWindow(20, 30));
    Modifications<KV<String, byte[]>> kvMods2 = kvTracker.getBundleModifications();
    assertNotSame(kvMods1, kvMods2);

    kvKey.set(KV.of(freshString("a"), utf8("1")));
    kvWindow.set(intervalWindow(10, 20));
    assertSame(kvMods1, kvTracker.getBundleModifications());

    assertEquals(2, kvStringCounter.encodeCount.get());
    assertEquals(2, windowCounter.encodeCount.get());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testNullAndNullableKeyCoalescing() {
    // VoidCoder with null keys
    AtomicReference<Void> voidKey = new AtomicReference<>(null);
    AtomicReference<BoundedWindow> window = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiTimerBundleTracker<Void> voidTracker =
        new FnApiTimerBundleTracker<>(
            VoidCoder.of(),
            (Coder<BoundedWindow>) (Coder) GlobalWindow.Coder.INSTANCE,
            voidKey::get,
            window::get);

    Modifications<Void> voidMods1 = voidTracker.getBundleModifications();
    voidTracker.timerModified(
        "t", TimeDomain.EVENT_TIME, timer(null, "", GlobalWindow.INSTANCE, 10, 10));
    Modifications<Void> voidMods2 = voidTracker.getBundleModifications();
    assertSame(voidMods1, voidMods2);

    // NullableCoder<String> across null -> "a" -> null -> "a"
    CountingCoder<String> nullableStringCoder =
        new CountingCoder<>(NullableCoder.of(StringUtf8Coder.of()));
    AtomicReference<String> strKey = new AtomicReference<>(null);
    FnApiTimerBundleTracker<String> strTracker =
        new FnApiTimerBundleTracker<>(
            nullableStringCoder,
            (Coder<BoundedWindow>) (Coder) GlobalWindow.Coder.INSTANCE,
            strKey::get,
            window::get);

    Modifications<String> nullMods = strTracker.getBundleModifications();
    Modifications<String> nullModsRepeat = strTracker.getBundleModifications();
    assertSame(nullMods, nullModsRepeat);

    strKey.set(freshString("a"));
    Modifications<String> aMods = strTracker.getBundleModifications();
    assertNotSame(nullMods, aMods);

    strKey.set(null);
    assertSame(nullMods, strTracker.getBundleModifications());

    strKey.set(freshString("a"));
    assertSame(aMods, strTracker.getBundleModifications());
    assertEquals(2, nullableStringCoder.encodeCount.get());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testInPlaceByteArrayKeyMutationDoesNotAliasTimers() {
    AtomicReference<byte[]> currentKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiTimerBundleTracker<byte[]> tracker =
        new FnApiTimerBundleTracker<>(
            ByteArrayCoder.of(),
            (Coder<BoundedWindow>) (Coder) GlobalWindow.Coder.INSTANCE,
            currentKey::get,
            currentWindow::get);

    byte[] reusedKey = utf8("key-a");
    currentKey.set(reusedKey);
    Modifications<byte[]> modsA = tracker.getBundleModifications();

    // Mutate in place to "key-b" and access again with both same array and fresh array
    reusedKey[4] = 'b';
    Modifications<byte[]> modsB = tracker.getBundleModifications();
    assertNotSame(modsA, modsB);

    currentKey.set(utf8("key-b"));
    assertSame(modsB, tracker.getBundleModifications());

    currentKey.set(utf8("key-a"));
    assertSame(modsA, tracker.getBundleModifications());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testResetClearsAndShrinksAfterLargeBundle() {
    AtomicReference<byte[]> currentKey = new AtomicReference<>();
    AtomicReference<BoundedWindow> currentWindow = new AtomicReference<>(GlobalWindow.INSTANCE);
    FnApiTimerBundleTracker<byte[]> tracker =
        new FnApiTimerBundleTracker<>(
            ByteArrayCoder.of(),
            (Coder<BoundedWindow>) (Coder) GlobalWindow.Coder.INSTANCE,
            currentKey::get,
            currentWindow::get);

    Modifications<byte[]> firstBundleKey0 = null;
    for (int i = 0; i < 100; i++) {
      byte[] k = utf8("k-" + i);
      currentKey.set(k);
      tracker.timerModified(
          "t", TimeDomain.EVENT_TIME, timer(k, "tag-" + i, GlobalWindow.INSTANCE, i, i));
      if (i == 0) {
        firstBundleKey0 = tracker.getBundleModifications();
      }
    }

    List<String> bundle1Timers = new ArrayList<>();
    tracker.outputTimers(id -> t -> bundle1Timers.add(t.getDynamicTimerTag()));
    assertEquals(100, bundle1Timers.size());

    tracker.reset();
    List<String> afterReset = new ArrayList<>();
    tracker.outputTimers(id -> t -> afterReset.add(t.getDynamicTimerTag()));
    assertEquals(0, afterReset.size());

    byte[] k0Next = utf8("k-0");
    currentKey.set(k0Next);
    Modifications<byte[]> secondBundleKey0 = tracker.getBundleModifications();
    assertNotSame(firstBundleKey0, secondBundleKey0);
  }
}
