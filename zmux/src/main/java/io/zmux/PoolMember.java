package io.zmux;

import java.time.Duration;

/**
 * A member of a pool ({@link PoolView#members()}): a pipe pool's member, or a connection of a connection pool.
 *
 * @param pipe a pipe pool's member: the ID of its first pipe; null for a connection
 * @param fateGroup a connection pool's connection: its fate group; 0 for a pipe pool's member
 * @param age the time since its oldest pipe was attached, or since the connection was established
 * @param idle a member: the least {@link PipeStats#sinceLastTraffic()} of its pipes (since attachment when none had
 *     traffic); a connection: zero while it carries pipes, else the time since its last pipe ended, or since it was
 *     established when it never carried one
 * @param pipes the number of its pipes: 1 for a dialled pipe, the group's size for a joined member, the pipes on a
 *     connection
 * @param bytesSent over its pipes; 0 for a connection
 * @param bytesReceived over its pipes; 0 for a connection
 * @param usable every pipe serves its direction and the peer attached it; for a connection, it takes new pipes
 * @param replacing a replacement of it is being dialled or joined
 */
public record PoolMember(PipeId pipe, long fateGroup, Duration age, Duration idle, int pipes, long bytesSent,
                         long bytesReceived, boolean usable, boolean replacing) {}
