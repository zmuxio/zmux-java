package io.zmux.internal.loop;

import io.zmux.internal.core.CarrierInfo;

/**
 * The immutable properties of a transport.
 *
 * @param carrier what the core needs: framing, TLS record sizes, lanes
 * @param applicationProtocol the authenticated application protocol, "" when none
 * @param connection the identity of the multi-stream connection the transport is a stream of, or null
 * @param receiveMemory the receive memory the carrier declared, charged once per connection
 * @param fateGroup the connection's default wire fate group, 0 when none
 * @param exporterLength the length of the session exporter (64 for split bootstrap)
 */
public record TransportInfo(CarrierInfo carrier, String applicationProtocol, Object connection, long receiveMemory,
                            long fateGroup, int exporterLength) {

    /** These properties with the carrier's datagram channel ch, which the core sees through them. */
    public TransportInfo withChannel(CarrierChannel ch) {
        var view = new io.zmux.internal.core.DatagramChannel() {
            @Override public long channelStreamId() { return ch.channelStreamId(); }
            @Override public int maxChannelDatagram() { return ch.maxChannelDatagram(); }
        };
        return new TransportInfo(carrier.withChannel(view), applicationProtocol, connection, receiveMemory, fateGroup, exporterLength);
    }

    /** The properties of a pipe input's carrier, as its capabilities declare them. */
    public static TransportInfo of(io.zmux.carrier.PipeIo pipe) {
        var caps = pipe.capabilities();
        boolean messages = pipe.framing() == io.zmux.carrier.Framing.MESSAGES;
        return new TransportInfo(new CarrierInfo(messages ? io.zmux.internal.core.Framing.RECORDS : io.zmux.internal.core.Framing.BYTES,
                false, false, caps.laneOpener() != null, false).withKind(pipe.carrier()).withLabel(pipe.label())
                .withWriteOverhead(caps.writeOverhead()), caps.applicationProtocol(), pipe.carrierConn(),
                caps.receiveMemory(), caps.fateGroup(), caps.exporter() == null ? 32 : caps.exporter().sessionLength());
    }
}
