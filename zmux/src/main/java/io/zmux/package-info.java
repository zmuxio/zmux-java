/**
 * Public API for the zmux v1 single-connection stream multiplexer.
 *
 * <p>The package exposes repository-default session, stream, send-stream, and
 * receive-stream interfaces for opening, accepting, reading, writing, and
 * closing multiplexed streams over one transport connection. The supported
 * integration surface lives in {@code io.zmux}; implementation helpers under
 * {@code io.zmux.internal} are not compatibility contracts.
 */
package io.zmux;
