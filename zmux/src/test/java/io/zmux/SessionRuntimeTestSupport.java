package io.zmux;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

final class SessionRuntimeTestSupport {
    private SessionRuntimeTestSupport() {
    }

    static SessionRuntime newRuntime(ZmuxConfig config) throws Exception {
        return newRuntime(
                new BasicDuplexConnection(emptyInput(), discardingOutput()),
                config
        );
    }

    static InputStream emptyInput() {
        return new ByteArrayInputStream(new byte[0]);
    }

    static OutputStream discardingOutput() {
        return new OutputStream() {
            @Override
            public void write(int value) {
            }

            @Override
            public void write(byte[] buffer, int offset, int length) {
            }
        };
    }

    static void queueWrite(Object stream, byte[] payload) throws IOException {
        ((StreamRuntime) stream).queueWrite(payload, 0, payload.length);
    }

    static int queueWriteFinal(Object stream, byte[] payload) throws IOException {
        return ((StreamRuntime) stream).queueWriteFinal(payload, 0, payload.length);
    }

    static int queueWritevFinal(Object stream, byte[]... parts) throws IOException {
        return ((StreamRuntime) stream).queueWritevFinal(parts);
    }

    static SessionRuntime newRuntime(io.zmux.DuplexConnection connection, ZmuxConfig config) throws Exception {
        Constructor<SessionRuntime> constructor = SessionRuntime.class.getDeclaredConstructor(io.zmux.DuplexConnection.class, ZmuxConfig.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                connection,
                config == null ? ZmuxConfig.builder().build() : config
        );
    }

    static SessionRuntime newReadyRuntime(long capabilities, Settings peerSettings) throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .capabilities(capabilities)
                .build();
        return newReadyRuntime(config, capabilities, peerSettings);
    }

    static SessionRuntime newReadyRuntime(io.zmux.DuplexConnection connection,
                                          ZmuxConfig config,
                                          long capabilities,
                                          Settings peerSettings) throws Exception {
        ZmuxConfig effectiveConfig = config == null
                ? ZmuxConfig.builder().role(Role.RESPONDER).capabilities(capabilities).build()
                : config;
        SessionRuntime runtime = newRuntime(connection, effectiveConfig);
        initializeReadyRuntime(runtime, effectiveConfig, capabilities, peerSettings);
        return runtime;
    }

    static SessionRuntime newReadyRuntime(ZmuxConfig config, long capabilities, Settings peerSettings) throws Exception {
        ZmuxConfig effectiveConfig = config == null
                ? ZmuxConfig.builder().role(Role.RESPONDER).capabilities(capabilities).build()
                : config;
        SessionRuntime runtime = newRuntime(effectiveConfig);
        initializeReadyRuntime(runtime, effectiveConfig, capabilities, peerSettings);
        return runtime;
    }

    private static void initializeReadyRuntime(SessionRuntime runtime,
                                               ZmuxConfig effectiveConfig,
                                               long capabilities,
                                               Settings peerSettings) throws Exception {
        Role localRole = Role.RESPONDER;
        Role peerRole = Role.INITIATOR;
        Preface peerPreface = new Preface(
                Protocol.PREFACE_VERSION,
                peerRole,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                capabilities,
                peerSettings
        );
        Negotiated negotiated = new Negotiated(
                Protocol.PROTO_VERSION,
                capabilities,
                localRole,
                peerRole,
                peerSettings
        );

        setField(runtime, "peerPreface", peerPreface);
        setField(runtime, "negotiated", negotiated);
        setField(runtime, "state", SessionState.READY);
        setLongField(runtime, "nextLocalBidi", SessionRuntime.firstLocalStreamId(localRole, true));
        setLongField(runtime, "nextLocalUni", SessionRuntime.firstLocalStreamId(localRole, false));
        setLongField(runtime, "nextPeerBidi", SessionRuntime.firstPeerStreamId(localRole, true));
        setLongField(runtime, "nextPeerUni", SessionRuntime.firstPeerStreamId(localRole, false));
        setLongField(runtime, "sessionSendLimit", peerSettings.initialMaxData());
        setLongField(runtime, "recvSessionAdvertised", effectiveConfig.settings().initialMaxData());
        long readyAtNanos = System.nanoTime();
        setLongField(runtime, "lastInboundFrameAtNanos", readyAtNanos);
        setLongField(runtime, "lastControlProgressAtNanos", readyAtNanos);
        setLongField(runtime, "lastTransportWriteAtNanos", readyAtNanos);
    }

    static Object newPendingPing(long startedAtNanos, byte[] payload) throws Exception {
        Class<?> type = Class.forName("io.zmux.SessionRuntime$PendingPing");
        Constructor<?> constructor = type.getDeclaredConstructor(long.class, byte[].class);
        constructor.setAccessible(true);
        return constructor.newInstance(startedAtNanos, payload);
    }

    static Object invokePrivate(Object target, String name, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        Object[] invocationArgs = args;
        if (invocationArgs == null) {
            invocationArgs = parameterTypes.length == 1 ? new Object[]{null} : new Object[parameterTypes.length];
        }
        return method.invoke(target, invocationArgs);
    }

    static void setField(Object target, String name, Object value) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        } catch (NoSuchFieldException noSuchFieldException) {
            if (!setSessionNestedField(target, name, value)) {
                throw noSuchFieldException;
            }
        }
    }

    static void setLongField(Object target, String name, long value) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.setLong(target, value);
        } catch (NoSuchFieldException noSuchFieldException) {
            if (!setSessionNestedLongField(target, name, value) && !setDelegatedLongField(target, name, value)) {
                throw noSuchFieldException;
            }
        }
    }

    static void setIntField(Object target, String name, int value) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.setInt(target, value);
        } catch (NoSuchFieldException noSuchFieldException) {
            if (!setSessionNestedIntField(target, name, value)) {
                throw noSuchFieldException;
            }
        }
    }

    static long getLongField(Object target, String name) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.getLong(target);
        } catch (NoSuchFieldException noSuchFieldException) {
            Long delegated = getSessionNestedLongField(target, name);
            if (delegated == null) {
                delegated = getDelegatedLongField(target, name);
            }
            if (delegated == null) {
                throw noSuchFieldException;
            }
            return delegated.longValue();
        }
    }

    @SuppressWarnings("unchecked")
    static Deque<Object> outboundQueue(SessionRuntime runtime, String name) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField(name);
        field.setAccessible(true);
        return (Deque<Object>) field.get(runtime);
    }

    static FrameCodec.Frame outboundFrame(Object outbound) throws Exception {
        return (FrameCodec.Frame) invokePrivate(outbound, "frame", new Class<?>[0]);
    }

    static long outboundStreamId(Object outbound) throws Exception {
        return outboundFrame(outbound).streamId();
    }

    static int outboundDataBytes(Object outbound) throws Exception {
        return (Integer) invokePrivate(outbound, "dataBytes", new Class<?>[0]);
    }

    static boolean outboundOpeningFrame(Object outbound) throws Exception {
        return (Boolean) invokePrivate(outbound, "openingFrame", new Class<?>[0]);
    }

    static StreamRuntime outboundStream(Object outbound) throws Exception {
        return (StreamRuntime) invokePrivate(outbound, "stream", new Class<?>[0]);
    }

    static byte[] outboundPayload(Object outbound) throws Exception {
        byte[] prefix = (byte[]) invokePrivate(outbound, "payloadPrefix", new Class<?>[0]);
        byte[] payloadBytes = (byte[]) invokePrivate(outbound, "payloadBytes", new Class<?>[0]);
        int payloadOffset = (Integer) invokePrivate(outbound, "payloadOffset", new Class<?>[0]);
        int payloadLength = (Integer) invokePrivate(outbound, "payloadLength", new Class<?>[0]);
        byte[][] payloadParts = (byte[][]) invokePrivate(outbound, "payloadParts", new Class<?>[0]);
        int payloadPartIndex = (Integer) invokePrivate(outbound, "payloadPartIndex", new Class<?>[0]);
        int payloadPartOffset = (Integer) invokePrivate(outbound, "payloadPartOffset", new Class<?>[0]);
        int prefixLength = prefix == null ? 0 : prefix.length;
        byte[] payload;
        if (payloadParts != null && payloadParts.length > 0 && payloadLength > 0) {
            payload = new byte[payloadLength];
            int index = payloadPartIndex;
            int offset = payloadPartOffset;
            int remaining = payloadLength;
            int written = 0;
            while (remaining > 0 && index < payloadParts.length) {
                byte[] part = payloadParts[index];
                if (offset >= part.length) {
                    index++;
                    offset = 0;
                    continue;
                }
                int copy = Math.min(part.length - offset, remaining);
                System.arraycopy(part, offset, payload, written, copy);
                written += copy;
                remaining -= copy;
                index++;
                offset = 0;
            }
        } else {
            payload = payloadBytes == null ? new byte[0] : Arrays.copyOfRange(payloadBytes, payloadOffset, payloadOffset + payloadLength);
        }
        if (prefixLength == 0) {
            return payload;
        }
        byte[] combined = new byte[prefixLength + payload.length];
        System.arraycopy(prefix, 0, combined, 0, prefixLength);
        System.arraycopy(payload, 0, combined, prefixLength, payload.length);
        return combined;
    }

    static int outboundPayloadPartCount(Object outbound) throws Exception {
        byte[][] payloadParts = (byte[][]) invokePrivate(outbound, "payloadParts", new Class<?>[0]);
        return payloadParts == null ? 0 : payloadParts.length;
    }

    static Object pollLastOutboundQueue(SessionRuntime runtime, String name) throws Exception {
        Deque<Object> queue = outboundQueue(runtime, name);
        Object outbound = queue.pollLast();
        if (outbound == null) {
            return null;
        }
        if (outboundFrame(outbound).type() == FrameType.DATA) {
            StreamRuntime stream = outboundStream(outbound);
            int dataBytes = outboundDataBytes(outbound);
            if (stream != null && dataBytes > 0) {
                Field sessionQueuedField = SessionRuntime.class.getDeclaredField("sessionQueuedDataBytes");
                sessionQueuedField.setAccessible(true);
                long sessionQueued = sessionQueuedField.getLong(runtime);
                sessionQueuedField.setLong(runtime, Math.max(0L, sessionQueued - dataBytes));
                stream.releaseQueuedDataBytesLocked(dataBytes);
            }
        }
        return outbound;
    }

    @SuppressWarnings("unchecked")
    static Deque<Object> advisoryQueue(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("advisoryQueue");
        field.setAccessible(true);
        return (Deque<Object>) field.get(runtime);
    }

    static List<Long> advisoryQueueStreamIds(SessionRuntime runtime) throws Exception {
        List<Long> streamIds = new ArrayList<>();
        for (Object item : advisoryQueue(runtime)) {
            if (item instanceof StreamRuntime) {
                StreamRuntime streamRuntime = (StreamRuntime) item;
                streamIds.add(streamRuntime.streamIdInternal());
            } else {
                streamIds.add(outboundStreamId(item));
            }
        }
        return streamIds;
    }

    private static boolean setDelegatedLongField(Object target, String name, long value) throws Exception {
        if (!(target instanceof StreamRuntime)) {
            return false;
        }
        if (!isDelegatedStreamLongField(name)) {
            return false;
        }
        return setNestedLongField((StreamRuntime) target, "sendAccountingState", name, value);
    }

    private static boolean setSessionNestedField(Object target, String name, Object value) throws Exception {
        for (Object owner : sessionNestedOwners(target)) {
            if (setNestedObjectField(owner, name, value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean setSessionNestedLongField(Object target, String name, long value) throws Exception {
        for (Object owner : sessionNestedOwners(target)) {
            if (setNestedLongField(owner, name, value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean setSessionNestedIntField(Object target, String name, int value) throws Exception {
        for (Object owner : sessionNestedOwners(target)) {
            if (setNestedIntField(owner, name, value)) {
                return true;
            }
        }
        return false;
    }

    private static Long getSessionNestedLongField(Object target, String name) throws Exception {
        for (Object owner : sessionNestedOwners(target)) {
            Long value = getNestedLongField(owner, name);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static List<Object> sessionNestedOwners(Object target) throws Exception {
        if (!(target instanceof SessionRuntime)) {
            return Collections.emptyList();
        }
        SessionRuntime runtime = (SessionRuntime) target;
        List<Object> owners = new ArrayList<>(6);
        addSessionOwner(runtime, owners, "telemetry");
        addSessionOwner(runtime, owners, "acceptRegistry");
        addSessionOwner(runtime, owners, "outboundQueueBookkeeping");
        addSessionOwner(runtime, owners, "flowControlUpdateRegistry");
        addSessionOwner(runtime, owners, "streamBookkeeping");
        return owners;
    }

    private static Long getDelegatedLongField(Object target, String name) throws Exception {
        if (!(target instanceof StreamRuntime) || !isDelegatedStreamLongField(name)) {
            return null;
        }
        return getNestedLongField((StreamRuntime) target, "sendAccountingState", name);
    }

    private static boolean isDelegatedStreamLongField(String name) {
        return "reservedSendBytes".equals(name)
                || "queuedDataBytes".equals(name)
                || "peerSendLimit".equals(name)
                || "blockedAt".equals(name)
                || "sentBytes".equals(name);
    }

    private static void addSessionOwner(SessionRuntime runtime, List<Object> owners, String fieldName) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        Object owner = field.get(runtime);
        if (owner != null) {
            owners.add(owner);
        }
    }

    private static boolean setNestedLongField(Object target, String ownerField, String nestedField, long value) throws Exception {
        Field field = target.getClass().getDeclaredField(ownerField);
        field.setAccessible(true);
        Object owner = field.get(target);
        if (owner == null) {
            return false;
        }
        Field nested = owner.getClass().getDeclaredField(nestedField);
        nested.setAccessible(true);
        nested.setLong(owner, value);
        return true;
    }

    private static boolean setNestedLongField(Object target, String nestedField, long value) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(nestedField);
            field.setAccessible(true);
            field.setLong(target, value);
            return true;
        } catch (NoSuchFieldException ignored) {
            return false;
        }
    }

    private static boolean setNestedIntField(Object target, String nestedField, int value) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(nestedField);
            field.setAccessible(true);
            field.setInt(target, value);
            return true;
        } catch (NoSuchFieldException ignored) {
            return false;
        }
    }

    private static boolean setNestedObjectField(Object target, String nestedField, Object value) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(nestedField);
            field.setAccessible(true);
            field.set(target, value);
            return true;
        } catch (NoSuchFieldException ignored) {
            return false;
        }
    }

    private static Long getNestedLongField(Object target, String ownerField, String nestedField) throws Exception {
        Field field = target.getClass().getDeclaredField(ownerField);
        field.setAccessible(true);
        Object owner = field.get(target);
        if (owner == null) {
            return null;
        }
        Field nested = owner.getClass().getDeclaredField(nestedField);
        nested.setAccessible(true);
        return nested.getLong(owner);
    }

    private static Long getNestedLongField(Object target, String nestedField) throws Exception {
        try {
            Field field = target.getClass().getDeclaredField(nestedField);
            field.setAccessible(true);
            return field.getLong(target);
        } catch (NoSuchFieldException ignored) {
            return null;
        }
    }
}
