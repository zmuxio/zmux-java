package io.zmux.helper;

/**
 * The datagram limiters of a session, one per direction ({@link io.zmux.ZmuxConfig.Builder#datagramLimiter}).
 * Each datagram's payload is asked {@code tryAcquire(n, n)} outside every lock: before it is queued
 * to send, and off the event loop before it enters the application's queue; one refused is dropped
 * and counted, one granted and then not queued is refunded. An empty datagram passes no limiter.
 *
 * @param send the limiter of datagrams sent; null: none
 * @param receive the limiter of datagrams received; null: none
 */
public record DatagramLimiter(Limiter send, Limiter receive) {}
