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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Memoizes encoded {@link ByteString} representations of the current key or window within a bundle,
 * using structural {@code byte[]} equality, supported composite structural codings, or {@link
 * Coder#consistentWithEquals()} so decoded duplicates do not trigger redundant re-encoding.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
final class MemoizedEncodedSupplier<T> implements Supplier<ByteString> {
  static final int MAX_RETAINED_CACHE_ENTRIES = 64;
  private static final Object NULL_SENTINEL = new Object();

  private enum Mode {
    BYTE_ARRAY,
    CONSISTENT_WITH_EQUALS,
    STRUCTURAL_COMPOSITE,
    IDENTITY_FALLBACK
  }

  /**
   * Reusable {@link ByteStringOutputStream} holder that resets partial writes on failure and drops
   * buffers that grow beyond {@link #MAX_RETAINED_BYTES} so cached bundle processors do not retain
   * oversized byte arrays indefinitely.
   */
  static final class EncodingBuffer {
    static final int MAX_RETAINED_BYTES = 4096;

    interface Encoder {
      void encode(ByteStringOutputStream out) throws IOException;
    }

    private @Nullable ByteStringOutputStream buffer;

    ByteString encode(Encoder encoder) {
      ByteStringOutputStream out = buffer;
      if (out == null) {
        out = new ByteStringOutputStream();
        buffer = out;
      }
      boolean clean = false;
      try {
        encoder.encode(out);
        if (out.size() > MAX_RETAINED_BYTES) {
          // Drop the buffer instead of calling toByteStringAndReset(), which would allocate a new
          // oversized backing array when wrapping the existing chunk.
          buffer = null;
          clean = true;
          return out.toByteString();
        }
        ByteString result = out.toByteStringAndReset();
        clean = true;
        return result;
      } catch (IOException e) {
        throw new IllegalStateException(e);
      } finally {
        if (!clean) {
          if (out.size() > MAX_RETAINED_BYTES) {
            buffer = null;
          } else {
            out.reset();
          }
        }
      }
    }
  }

  private abstract static class ByteArrayCacheKey {
    byte @Nullable [] bytes;
    int hash;

    @Override
    public final int hashCode() {
      return hash;
    }

    @Override
    public final boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ByteArrayCacheKey)) {
        return false;
      }
      ByteArrayCacheKey other = (ByteArrayCacheKey) o;
      return hash == other.hash && Arrays.equals(bytes, other.bytes);
    }
  }

  private static final class ImmutableByteArrayKey extends ByteArrayCacheKey {
    ImmutableByteArrayKey(byte[] bytes) {
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
    }
  }

  private static final class MutableByteArrayLookupKey extends ByteArrayCacheKey {
    void set(byte[] bytes) {
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
    }

    void reset() {
      this.bytes = null;
      this.hash = 0;
    }
  }

  private final Supplier<T> argSupplier;
  private final @Nullable Coder<?> coder;
  private final Function<T, ByteString> encodeFn;
  private final Mode mode;

  private @Nullable MutableByteArrayLookupKey byteArrayLookupProbe;
  private @Nullable T memoizedArg;
  private @Nullable Object memoizedStructuralKey;
  private @Nullable ByteString memoizedResult;
  private int memoizedByteArrayLength;
  private boolean initialized;
  private @Nullable Map<Object, ByteString> encodedValuesCache;

  MemoizedEncodedSupplier(
      Supplier<T> argSupplier, @Nullable Coder<T> coder, Function<T, ByteString> encodeFn) {
    this.argSupplier = argSupplier;
    this.coder = coder;
    this.encodeFn = encodeFn;
    if (coder != null && unwrapLengthPrefix(coder) instanceof ByteArrayCoder) {
      this.mode = Mode.BYTE_ARRAY;
    } else if (coder != null && isConsistentWithEqualsOrVoid(coder)) {
      this.mode = Mode.CONSISTENT_WITH_EQUALS;
    } else if (coder != null && isSupportedCompositeCoder(coder)) {
      this.mode = Mode.STRUCTURAL_COMPOSITE;
    } else {
      this.mode = Mode.IDENTITY_FALLBACK;
    }
  }

  @Override
  public ByteString get() {
    T currentArg = argSupplier.get();
    switch (mode) {
      case BYTE_ARRAY:
        return getByteArray(currentArg);
      case CONSISTENT_WITH_EQUALS:
        return getConsistentWithEquals(currentArg);
      case STRUCTURAL_COMPOSITE:
        return getStructuralComposite(currentArg);
      case IDENTITY_FALLBACK:
      default:
        return getIdentityFallback(currentArg);
    }
  }

  private ByteString getByteArray(@Nullable T currentArg) {
    if (currentArg == null) {
      return encodeFn.apply(null);
    }
    byte[] currentBytes = (byte[]) currentArg;
    if (!initialized) {
      ByteString encoded = encodeFn.apply(currentArg);
      this.memoizedByteArrayLength = currentBytes.length;
      this.memoizedResult = encoded;
      this.initialized = true;
      return encoded;
    }
    if (matchesMemoizedEncodedByteArray(currentBytes, memoizedByteArrayLength, memoizedResult)) {
      return memoizedResult;
    }
    if (encodedValuesCache == null) {
      ByteString encoded = encodeFn.apply(currentArg);
      Map<Object, ByteString> cache = new HashMap<>();
      seedPreviousByteArrayEntry(cache);
      cache.put(new ImmutableByteArrayKey(currentBytes.clone()), encoded);
      this.encodedValuesCache = cache;
      if (this.byteArrayLookupProbe == null) {
        this.byteArrayLookupProbe = new MutableByteArrayLookupKey();
      }
      this.memoizedByteArrayLength = currentBytes.length;
      this.memoizedResult = encoded;
      return encoded;
    }
    MutableByteArrayLookupKey probe = byteArrayLookupProbe;
    if (probe == null) {
      probe = new MutableByteArrayLookupKey();
      this.byteArrayLookupProbe = probe;
    }
    ByteString cached;
    probe.set(currentBytes);
    try {
      cached = encodedValuesCache.get(probe);
    } finally {
      probe.reset();
    }
    if (cached != null) {
      this.memoizedByteArrayLength = currentBytes.length;
      this.memoizedResult = cached;
      return cached;
    }
    ByteString encoded = encodeFn.apply(currentArg);
    if (encodedValuesCache.isEmpty()) {
      seedPreviousByteArrayEntry(encodedValuesCache);
    }
    encodedValuesCache.put(new ImmutableByteArrayKey(currentBytes.clone()), encoded);
    this.memoizedByteArrayLength = currentBytes.length;
    this.memoizedResult = encoded;
    return encoded;
  }

  private void seedPreviousByteArrayEntry(Map<Object, ByteString> cache) {
    if (memoizedResult == null) {
      return;
    }
    int prevPrefixLen = memoizedResult.size() - memoizedByteArrayLength;
    if (prevPrefixLen >= 0) {
      byte[] prevBytes = memoizedResult.substring(prevPrefixLen).toByteArray();
      cache.put(new ImmutableByteArrayKey(prevBytes), memoizedResult);
    }
  }

  private ByteString getConsistentWithEquals(@Nullable T currentArg) {
    if (!initialized) {
      ByteString encoded = encodeFn.apply(currentArg);
      this.memoizedArg = currentArg;
      this.memoizedResult = encoded;
      this.initialized = true;
      return encoded;
    }
    if (Objects.equals(currentArg, memoizedArg)) {
      this.memoizedArg = currentArg;
      return memoizedResult;
    }
    if (encodedValuesCache != null) {
      ByteString cached = encodedValuesCache.get(currentArg);
      if (cached != null) {
        this.memoizedArg = currentArg;
        this.memoizedResult = cached;
        return cached;
      }
    }
    ByteString encoded = encodeFn.apply(currentArg);
    if (encodedValuesCache == null) {
      encodedValuesCache = new HashMap<>();
    }
    if (encodedValuesCache.isEmpty()) {
      encodedValuesCache.put(memoizedArg, memoizedResult);
    }
    encodedValuesCache.put(currentArg, encoded);
    this.memoizedArg = currentArg;
    this.memoizedResult = encoded;
    return encoded;
  }

  private ByteString getStructuralComposite(@Nullable T currentArg) {
    if (!initialized) {
      ByteString encoded = encodeFn.apply(currentArg);
      if (canBuildStructuralKey(coder, currentArg)) {
        this.memoizedStructuralKey = toStructuralKey(coder, currentArg, true);
      } else {
        this.memoizedStructuralKey = null;
      }
      this.memoizedResult = encoded;
      this.initialized = true;
      return encoded;
    }
    if (matchesStructuralKey(coder, currentArg, memoizedStructuralKey)) {
      return memoizedResult;
    }
    if (!canBuildStructuralKey(coder, currentArg)) {
      ByteString encoded = encodeFn.apply(currentArg);
      this.memoizedStructuralKey = null;
      this.memoizedResult = encoded;
      return encoded;
    }
    if (encodedValuesCache != null) {
      Object lookupKey = toStructuralKey(coder, currentArg, false);
      ByteString cached = encodedValuesCache.get(lookupKey);
      if (cached != null) {
        this.memoizedStructuralKey = toStructuralKey(coder, currentArg, true);
        this.memoizedResult = cached;
        return cached;
      }
    }
    ByteString encoded = encodeFn.apply(currentArg);
    Object immutableKey = toStructuralKey(coder, currentArg, true);
    if (encodedValuesCache == null) {
      encodedValuesCache = new HashMap<>();
    }
    if (encodedValuesCache.isEmpty() && memoizedStructuralKey != null && memoizedResult != null) {
      encodedValuesCache.put(memoizedStructuralKey, memoizedResult);
    }
    encodedValuesCache.put(immutableKey, encoded);
    this.memoizedStructuralKey = immutableKey;
    this.memoizedResult = encoded;
    return encoded;
  }

  private ByteString getIdentityFallback(@Nullable T currentArg) {
    if (!initialized) {
      ByteString encoded = encodeFn.apply(currentArg);
      this.memoizedArg = currentArg;
      this.memoizedResult = encoded;
      this.initialized = true;
      return encoded;
    }
    if (currentArg == memoizedArg) {
      return memoizedResult;
    }
    ByteString encoded = encodeFn.apply(currentArg);
    if (encoded.equals(memoizedResult)) {
      encoded = memoizedResult;
    }
    this.memoizedArg = currentArg;
    this.memoizedResult = encoded;
    return encoded;
  }

  void reset() {
    this.memoizedArg = null;
    this.memoizedStructuralKey = null;
    this.memoizedResult = null;
    this.memoizedByteArrayLength = 0;
    this.initialized = false;
    if (byteArrayLookupProbe != null) {
      byteArrayLookupProbe.reset();
    }
    if (encodedValuesCache != null) {
      if (encodedValuesCache.size() > MAX_RETAINED_CACHE_ENTRIES) {
        encodedValuesCache = null;
      } else if (!encodedValuesCache.isEmpty()) {
        encodedValuesCache.clear();
      }
    }
  }

  private static Coder<?> unwrapLengthPrefix(Coder<?> coder) {
    Coder<?> current = coder;
    while (current instanceof LengthPrefixCoder) {
      current = ((LengthPrefixCoder<?>) current).getValueCoder();
    }
    return current;
  }

  private static boolean isConsistentWithEqualsOrVoid(Coder<?> coder) {
    if (coder instanceof VoidCoder || coder.consistentWithEquals()) {
      return true;
    }
    if (coder instanceof LengthPrefixCoder) {
      return isConsistentWithEqualsOrVoid(((LengthPrefixCoder<?>) coder).getValueCoder());
    }
    return false;
  }

  private static boolean isSupportedCompositeCoder(Coder<?> coder) {
    Coder<?> unwrapped = unwrapLengthPrefix(coder);
    if (unwrapped.consistentWithEquals()
        || unwrapped instanceof VoidCoder
        || unwrapped instanceof ByteArrayCoder) {
      return true;
    }
    if (unwrapped instanceof NullableCoder) {
      return isSupportedCompositeCoder(((NullableCoder<?>) unwrapped).getValueCoder());
    }
    if (unwrapped instanceof KvCoder) {
      KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) unwrapped;
      return isSupportedCompositeCoder(kvCoder.getKeyCoder())
          && isSupportedCompositeCoder(kvCoder.getValueCoder());
    }
    if (unwrapped instanceof ShardedKey.Coder) {
      return isSupportedCompositeCoder(((ShardedKey.Coder<?>) unwrapped).getKeyCoder());
    }
    return false;
  }

  private static boolean matchesMemoizedEncodedByteArray(
      byte[] currentBytes, int memoizedLen, @Nullable ByteString encoded) {
    if (encoded == null || currentBytes.length != memoizedLen) {
      return false;
    }
    int prefixLen = encoded.size() - memoizedLen;
    if (prefixLen < 0) {
      return false;
    }
    for (int i = 0; i < memoizedLen; i++) {
      if (encoded.byteAt(prefixLen + i) != currentBytes[i]) {
        return false;
      }
    }
    return true;
  }

  private static boolean canBuildStructuralKey(Coder<?> coder, @Nullable Object value) {
    Coder<?> unwrapped = unwrapLengthPrefix(coder);
    if (unwrapped instanceof NullableCoder) {
      return value == null
          || canBuildStructuralKey(((NullableCoder<?>) unwrapped).getValueCoder(), value);
    }
    if (unwrapped instanceof VoidCoder) {
      return value == null;
    }
    if (value == null) {
      return false;
    }
    if (unwrapped.consistentWithEquals()) {
      return true;
    }
    if (unwrapped instanceof ByteArrayCoder) {
      return value instanceof byte[];
    }
    if (unwrapped instanceof KvCoder) {
      if (!(value instanceof KV)) {
        return false;
      }
      KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) unwrapped;
      KV<?, ?> kv = (KV<?, ?>) value;
      return canBuildStructuralKey(kvCoder.getKeyCoder(), kv.getKey())
          && canBuildStructuralKey(kvCoder.getValueCoder(), kv.getValue());
    }
    if (unwrapped instanceof ShardedKey.Coder) {
      if (!(value instanceof ShardedKey)) {
        return false;
      }
      return canBuildStructuralKey(
          ((ShardedKey.Coder<?>) unwrapped).getKeyCoder(), ((ShardedKey<?>) value).getKey());
    }
    return false;
  }

  private static Object toStructuralKey(Coder<?> coder, @Nullable Object value, boolean copyBytes) {
    Coder<?> unwrapped = unwrapLengthPrefix(coder);
    if (unwrapped instanceof NullableCoder) {
      if (value == null) {
        return NULL_SENTINEL;
      }
      return toStructuralKey(((NullableCoder<?>) unwrapped).getValueCoder(), value, copyBytes);
    }
    if (unwrapped instanceof VoidCoder) {
      return NULL_SENTINEL;
    }
    if (unwrapped.consistentWithEquals()) {
      return value;
    }
    if (unwrapped instanceof ByteArrayCoder) {
      byte[] bytes = (byte[]) value;
      return new ImmutableByteArrayKey(copyBytes ? bytes.clone() : bytes);
    }
    if (unwrapped instanceof KvCoder) {
      KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) unwrapped;
      KV<?, ?> kv = (KV<?, ?>) value;
      return KV.of(
          toStructuralKey(kvCoder.getKeyCoder(), kv.getKey(), copyBytes),
          toStructuralKey(kvCoder.getValueCoder(), kv.getValue(), copyBytes));
    }
    if (unwrapped instanceof ShardedKey.Coder) {
      return ((ShardedKey.Coder) unwrapped).structuralValue(value);
    }
    throw new IllegalStateException("Unsupported coder for structural key: " + coder);
  }

  private static boolean matchesStructuralKey(
      Coder<?> coder, @Nullable Object currentArg, @Nullable Object storedKey) {
    if (storedKey == null) {
      return false;
    }
    Coder<?> unwrapped = unwrapLengthPrefix(coder);
    if (unwrapped instanceof NullableCoder) {
      if (currentArg == null) {
        return storedKey == NULL_SENTINEL;
      }
      if (storedKey == NULL_SENTINEL) {
        return false;
      }
      return matchesStructuralKey(
          ((NullableCoder<?>) unwrapped).getValueCoder(), currentArg, storedKey);
    }
    if (unwrapped instanceof VoidCoder) {
      return currentArg == null && storedKey == NULL_SENTINEL;
    }
    if (currentArg == null) {
      return false;
    }
    if (unwrapped.consistentWithEquals()) {
      return currentArg.equals(storedKey);
    }
    if (unwrapped instanceof ByteArrayCoder) {
      return currentArg instanceof byte[]
          && storedKey instanceof ByteArrayCacheKey
          && Arrays.equals((byte[]) currentArg, ((ByteArrayCacheKey) storedKey).bytes);
    }
    if (unwrapped instanceof KvCoder) {
      if (!(currentArg instanceof KV) || !(storedKey instanceof KV)) {
        return false;
      }
      KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) unwrapped;
      KV<?, ?> currentKv = (KV<?, ?>) currentArg;
      KV<?, ?> storedKv = (KV<?, ?>) storedKey;
      return matchesStructuralKey(kvCoder.getKeyCoder(), currentKv.getKey(), storedKv.getKey())
          && matchesStructuralKey(
              kvCoder.getValueCoder(), currentKv.getValue(), storedKv.getValue());
    }
    if (unwrapped instanceof ShardedKey.Coder) {
      if (!(currentArg instanceof ShardedKey)) {
        return false;
      }
      return ((ShardedKey.Coder) unwrapped).structuralValue(currentArg).equals(storedKey);
    }
    return false;
  }
}
