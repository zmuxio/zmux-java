package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.Zmux;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class StreamingTest {
    @Test void bidirectionalOverHttp1() throws Exception {
        try (var f = Fixture.plain(b -> b.streaming()); var c = f.client(b -> b.http1())) {
            try (var s = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config)) {
                f.echo(s);
            }
        }
    }

    @Test void bidirectionalOverH2cPriorKnowledge() throws Exception {
        try (var f = Fixture.plain(b -> b.streaming()); var c = f.client(b -> b.http2())) {
            try (var s = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config)) {
                f.echo(s);
            }
        }
    }

    @Test void bidirectionalOverTlsH2AndHttp1() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.streaming(), true, null)) {
            for (boolean h1 : new boolean[] {false, true}) {
                try (var c = f.client(b -> h1 ? b.http1() : b)) {
                    try (var s = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config)) {
                        f.echo(s);
                    }
                }
            }
        }
    }

    @Test void splitBootstrap() throws Exception {
        try (var f = Fixture.plain(b -> b.streaming())) {
            for (boolean h1 : new boolean[] {true, false}) {
                try (var c = f.client(b -> h1 ? b.http1() : b.http2())) {
                    try (var s = Zmux.open(c.splitPipe(f.base, Map.of(), null), Role.CLIENT, f.config)) {
                        f.echo(s);
                    }
                }
            }
        }
    }
}
