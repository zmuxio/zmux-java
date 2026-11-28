package io.zmux;

import java.net.InetSocketAddress;

/** Authorizes an authenticated inbound Join off the session loop. */
@FunctionalInterface
public interface AuthorizeJoin {
    /** The direction is relative to the accepting endpoint. */
    record Request(long authGeneration, SessionId session, PipeId pipe, long epoch, PipeOptions.Direction direction,
                   RecordMode mode, CarrierKind carrier, InetSocketAddress source, int firstBatchBytes) {}

    boolean authorize(Request request);
}
