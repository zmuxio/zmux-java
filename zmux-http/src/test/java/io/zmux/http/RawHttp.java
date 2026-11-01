package io.zmux.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/** Sends one raw HTTP/1.1 request and reads the status line and what follows until the peer closes or the read times out. */
final class RawHttp {
    static String send(URI base, String request, byte[] body) throws IOException {
        try (Socket s = new Socket(base.getHost(), base.getPort())) {
            s.setSoTimeout(3000);
            s.getOutputStream().write(request.getBytes(StandardCharsets.ISO_8859_1));
            if (body != null) s.getOutputStream().write(body);
            s.getOutputStream().flush();
            InputStream in = s.getInputStream();
            var out = new ByteArrayOutputStream();
            byte[] b = new byte[4096];
            try {
                for (int n; (n = in.read(b)) > 0; ) {
                    out.write(b, 0, n);
                    String got = out.toString(StandardCharsets.ISO_8859_1);
                    int end = got.indexOf("\r\n\r\n");
                    if (end > 0 && (got.contains("Content-Length: 0") || got.endsWith("host") || got.contains(" 425 "))) break;
                }
            } catch (java.net.SocketTimeoutException e) {
                // What arrived is the answer.
            }
            return out.toString(StandardCharsets.ISO_8859_1);
        }
    }
}
