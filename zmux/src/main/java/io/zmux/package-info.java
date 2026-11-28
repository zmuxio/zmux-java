/**
 * zmux: QMux-compatible multiplexing of streams and datagrams over one or more carriers.
 *
 * <h2>Terms</h2>
 *
 * <dl>
 *   <dt>carrier</dt>
 *   <dd>One underlying transport object that moves bytes or messages: a TCP or TLS connection, a
 *       QUIC stream or connection, an HTTP request or response body, a sequence of HTTP exchange
 *       requests, a WebSocket, a stream of another zmux session. It has its own handshake,
 *       addresses, admission and failure. A <em>pipe input</em> ({@link io.zmux.carrier.PipeIo})
 *       is the carrier, or the pair of carriers of a split, handed to a session to become one
 *       pipe. A carrier is never called a pipe.</dd>
 *   <dt>pipe</dt>
 *   <dd>One path of a session between the two endpoints ({@link io.zmux.ZmuxPipe}), named by a
 *       pipe ID and carried by one carrier or a group of carriers; each of its at most two
 *       directions is its outbound or inbound direction.</dd>
 *   <dt>pipe ID</dt>
 *   <dd>The identifier of a pipe ({@link io.zmux.PipeId}), unique for the life of the session, 0
 *       for the bootstrap pipe; its own type, never mixed with other integers.</dd>
 *   <dt>lane</dt>
 *   <dd>A further stream of the multi-stream connection a pipe's carrier stream belongs to,
 *       carrying records of one direction of that pipe beside it; lane 0 is the carrier stream
 *       itself. A lane is part of its pipe: no pipe ID, events or statistics of its own.</dd>
 *   <dt>session</dt>
 *   <dd>One zmux connection between two endpoints ({@link io.zmux.ZmuxSession}): its streams, its
 *       flow control, its pipes.</dd>
 *   <dt>stream</dt>
 *   <dd>A QUIC-semantics stream of a session; streams do not belong to pipes.</dd>
 *   <dt>endpoint, role</dt>
 *   <dd>One side of a session; its {@link io.zmux.Role} is client, server or auto.</dd>
 *   <dt>joiner, acceptor</dt>
 *   <dd>The endpoint that created a pipe, and the other one.</dd>
 *   <dt>listener</dt>
 *   <dd>The server-side registry of sessions ({@link io.zmux.ZmuxListener}): it takes carriers,
 *       admits them, and classifies them as new sessions, joins or resumptions.</dd>
 *   <dt>admission</dt>
 *   <dd>The listener's decision on a new carrier before the session layer sees it
 *       ({@link io.zmux.Admission}, {@link io.zmux.AdmissionVerdict}); a client presents a
 *       {@link io.zmux.CarrierCredential} to it.</dd>
 *   <dt>secure carrier</dt>
 *   <dd>The carrier transform of {@link io.zmux.seal}; its peer judgement is a
 *       {@code VerifyVerdict}.</dd>
 *   <dt>record mode</dt>
 *   <dd>How a pipe's records are protected ({@link io.zmux.RecordMode}).</dd>
 *   <dt>carrier choice</dt>
 *   <dd>The dialer's choice among carrier kinds ({@link io.zmux.SessionController}), never called a
 *       mode.</dd>
 *   <dt>fate group, key group</dt>
 *   <dd>A number the joiner gives its pipes that are expected to fail together; the key-exchange
 *       group of a pipe's join.</dd>
 *   <dt>epoch</dt>
 *   <dd>A generation of the session's join keys.</dd>
 * </dl>
 *
 * <h2>Settings</h2>
 *
 * Not calling a builder method keeps the default; a setting that is off, none, no timeout or
 * unlimited has a {@code noX()} method of its own; no number stands for any of them.
 */
package io.zmux;
