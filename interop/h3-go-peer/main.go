// This peer exercises the locked zmuxh3 HTTP/3 transport and its production
// WebTransport dependency at the wire boundary.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxh3"
)

const deadline = 15 * time.Second
const splitHeader = "Zmux-Pipe-Pair"
const splitToken = "d2lyZS1wZWVyLXBhaXItMQ"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 5 || (args[1] != "h3" && args[1] != "wt") {
		return errors.New("usage: h3-go-peer server|client h3|wt PORT CERT KEY")
	}
	address := net.JoinHostPort("127.0.0.1", args[2])
	// Process deadline includes handshake, all stream operations, and cleanup.
	hard := time.AfterFunc(deadline, func() {
		fmt.Fprintln(os.Stderr, "Go peer hard deadline exceeded")
		os.Exit(1)
	})
	defer hard.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	switch args[0] {
	case "server":
		keypair, err := tls.LoadX509KeyPair(args[3], args[4])
		if err != nil {
			return err
		}
		return serve(ctx, address, &tls.Config{Certificates: []tls.Certificate{keypair}}, args[1] == "wt")
	case "client":
		cert, err := os.ReadFile(args[3])
		if err != nil {
			return err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(cert) {
			return errors.New("invalid certification PEM")
		}
		config := &tls.Config{RootCAs: roots}
		url := "https://" + address
		if args[1] == "wt" {
			return runWTClient(ctx, url+"/wt", config)
		}
		return runHTTPClient(ctx, url, config)
	default:
		return fmt.Errorf("unknown role %q", args[0])
	}
}

func wtConfig() *webtransport.Config {
	return &webtransport.Config{MaxIncomingStreams: 8, MaxIncomingUniStreams: 8, MaxIncomingData: 65536}
}

func serve(ctx context.Context, addr string, config *tls.Config, wt bool) error {
	packet, err := net.ListenPacket("udp4", addr)
	if err != nil {
		return err
	}
	defer packet.Close()
	results := make(chan error, 1)
	h3 := zmuxh3.NewServer(addr, config, nil, http.NotFoundHandler(), nil)
	defer h3.Close()
	serve := h3.Serve
	if wt {
		h3.TLSConfig = &tls.Config{Certificates: config.Certificates, NextProtos: []string{http3.NextProtoH3}}
		server := &webtransport.Server{H3: h3, Config: wtConfig()}
		defer server.Close()
		h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/health" {
				_, _ = io.WriteString(w, "ok")
				return
			}
			if r.URL.Path != "/wt" {
				http.NotFound(w, r)
				return
			}
			session, err := server.Upgrade(w, r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			} else {
				settings := w.(http3.Settingser).Settings()
				fmt.Printf("WT CONNECT protocol=%s h3_datagram=%t peer_settings=%v\n",
					r.Proto, settings.EnableDatagrams, settings.Other)
				err = runWTServer(ctx, session)
			}
			select {
			case results <- err:
			default:
			}
		})
		serve = server.Serve
	} else {
		h3.Handler = httpHandler()
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- serve(packet) }()
	fmt.Printf("READY %d\n", packet.LocalAddr().(*net.UDPAddr).Port)
	for {
		select {
		case err := <-serverErrors:
			return err
		case err := <-results:
			if err != nil {
				return err
			}
			fmt.Println("PASS wt server close=42 reason=done")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func httpHandler() http.Handler {
	var pairs sync.Map
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 3 {
			http.Error(w, "HTTP/3 required", http.StatusHTTPVersionNotSupported)
			return
		}
		switch {
		case r.URL.Path == "/health" && r.Method == http.MethodGet:
			_, _ = io.WriteString(w, "ok")
		case r.URL.Path == "/stream" && r.Method == http.MethodPost && r.ContentLength == -1:
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
			echoChunks(w, r.Body)
		case r.URL.Path == "/exchange" && r.Method == http.MethodPost && r.ContentLength >= 0:
			_, _ = io.Copy(w, r.Body)
		case r.URL.Path == "/split":
			token := r.Header.Get(splitHeader)
			decoded, err := base64.RawURLEncoding.Strict().DecodeString(token)
			if err != nil || len(decoded) != 16 || len(token) != base64.RawURLEncoding.EncodedLen(16) {
				http.Error(w, "invalid split token", http.StatusBadRequest)
				return
			}
			pair, _ := pairs.LoadOrStore(token, make(chan []byte, 1))
			split := pair.(chan []byte)
			switch r.Method {
			case http.MethodGet:
				select {
				case body := <-split:
					pairs.CompareAndDelete(token, split)
					_, _ = w.Write(body)
				case <-r.Context().Done():
				}
			case http.MethodPost:
				body, err := io.ReadAll(io.LimitReader(r.Body, 1024))
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				select {
				case split <- body:
					w.WriteHeader(http.StatusOK)
				case <-r.Context().Done():
				}
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		default:
			http.Error(w, "unexpected request shape", http.StatusBadRequest)
		}
	})
}

func echoChunks(w http.ResponseWriter, body io.Reader) {
	var buffer [1024]byte
	controller := http.NewResponseController(w)
	for {
		n, readErr := body.Read(buffer[:])
		if n > 0 {
			if _, err := w.Write(buffer[:n]); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

func runHTTPClient(ctx context.Context, base string, config *tls.Config) error {
	client, transport := zmuxh3.HTTPClient(config, nil)
	defer transport.Close()
	do := func(method, path string, body io.Reader) (*http.Response, error) {
		request, err := http.NewRequestWithContext(ctx, method, base+path, body)
		if err != nil {
			return nil, err
		}
		if path == "/split" {
			request.Header.Set(splitHeader, splitToken)
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK || response.ProtoMajor != 3 {
			response.Body.Close()
			return nil, fmt.Errorf("%s %s: %s over %s", method, path, response.Status, response.Proto)
		}
		return response, nil
	}
	check := func(method, path string, input io.Reader, expected string) error {
		response, err := do(method, path, input)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		return expectEOF(response.Body, expected)
	}

	if err := checkStreaming(do); err != nil {
		return fmt.Errorf("streaming: %w", err)
	}
	if err := check(http.MethodPost, "/exchange", strings.NewReader("exchange"), "exchange"); err != nil {
		return fmt.Errorf("exchange: %w", err)
	}
	down := make(chan error, 1)
	go func() { down <- check(http.MethodGet, "/split", nil, "split") }()
	if err := check(http.MethodPost, "/split", strings.NewReader("split"), ""); err != nil {
		return fmt.Errorf("split upload: %w", err)
	}
	if err := <-down; err != nil {
		return fmt.Errorf("split download: %w", err)
	}
	if err := check(http.MethodGet, "/health", nil, "ok"); err != nil {
		return fmt.Errorf("health: %w", err)
	}
	fmt.Println("PASS h3 client streaming=stream split=split exchange=exchange")
	return nil
}

func checkStreaming(do func(string, string, io.Reader) (*http.Response, error)) error {
	body, writer := io.Pipe()
	defer body.Close()
	defer writer.Close()
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, "str")
		written <- err
	}()
	response, err := do(http.MethodPost, "/stream", body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var prefix [3]byte
	if _, err := io.ReadFull(response.Body, prefix[:]); err != nil {
		return err
	}
	if err := <-written; err != nil {
		return err
	}
	if string(prefix[:]) != "str" {
		return fmt.Errorf("received %q before request FIN, want %q", prefix, "str")
	}
	if err := sendFIN(writer, "eam"); err != nil {
		return err
	}
	return expectEOF(response.Body, "eam")
}

func expectEOF(r io.Reader, expected string) error {
	data, err := io.ReadAll(io.LimitReader(r, int64(len(expected)+1)))
	if err != nil {
		return err
	}
	if !bytes.Equal(data, []byte(expected)) {
		return fmt.Errorf("received %q, expected %q and FIN", data, expected)
	}
	return nil
}

func sendFIN(w io.WriteCloser, payload string) error {
	if _, err := io.WriteString(w, payload); err != nil {
		return err
	}
	return w.Close()
}

func runWTServer(ctx context.Context, session *webtransport.Session) error {
	stream, err := session.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if err := expectEOF(stream, "client-bidi"); err != nil {
		return err
	}
	if err := sendFIN(stream, "client-bidi"); err != nil {
		return err
	}
	incoming, err := session.AcceptUniStream(ctx)
	if err != nil {
		return err
	}
	if err := expectEOF(incoming, "client-uni"); err != nil {
		return err
	}
	outgoing, err := session.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := sendFIN(outgoing, "server-uni"); err != nil {
		return err
	}
	stream, err = session.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := sendFIN(stream, "server-bidi"); err != nil {
		return err
	}
	if err := expectEOF(stream, "server-bidi"); err != nil {
		return err
	}
	// Accept reports the WebTransport close cause. Session.Context only
	// signals cancellation and does not carry the capsule's code.
	unexpected, cause := session.AcceptUniStream(ctx)
	if unexpected != nil {
		unexpected.CancelRead(0)
		return errors.New("unexpected stream while waiting for CLOSE_WEBTRANSPORT_SESSION")
	}
	var closeErr *webtransport.SessionError
	if !errors.As(cause, &closeErr) || !closeErr.Remote || closeErr.ErrorCode != 42 || closeErr.Message != "done" {
		return fmt.Errorf("unexpected CONNECT termination: %v", cause)
	}
	return nil
}

// The fixture uses DialSession's credentialed path for its adapter-owned QUIC
// lifetime, including the linger that allows the close capsule to reach the peer.
type wtCredential struct{}

func (wtCredential) Prefix(*zmux.CarrierInfo) ([]byte, error) { return nil, nil }

func (wtCredential) Header(*zmux.CarrierInfo) (string, string, error) {
	return "X-Zmux-Wire-Peer", "go", nil
}

func runWTClient(ctx context.Context, rawURL string, config *tls.Config) error {
	var conn *quic.Conn
	transport := &webtransport.Transport{
		TLSClientConfig: config,
		Config:          wtConfig(),
		DialAddr: func(ctx context.Context, addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Conn, error) {
			var err error
			conn, err = quic.DialAddr(ctx, addr, tlsConfig, quicConfig)
			return conn, err
		},
	}
	session, err := zmuxh3.DialSession(ctx, transport, rawURL, nil, wtCredential{})
	if err != nil {
		return fmt.Errorf("WT CONNECT: %w", err)
	}
	defer transport.Close()
	defer conn.CloseWithError(0, "")
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := sendFIN(stream, "client-bidi"); err != nil {
		return err
	}
	if err := expectEOF(stream, "client-bidi"); err != nil {
		return err
	}
	outgoing, err := session.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := sendFIN(outgoing, "client-uni"); err != nil {
		return err
	}
	incoming, err := session.AcceptUniStream(ctx)
	if err != nil {
		return err
	}
	if err := expectEOF(incoming, "server-uni"); err != nil {
		return err
	}
	stream, err = session.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if err := expectEOF(stream, "server-bidi"); err != nil {
		return err
	}
	if err := sendFIN(stream, "server-bidi"); err != nil {
		return err
	}
	if err := session.CloseWithError(42, "done"); err != nil {
		return err
	}
	select {
	case <-conn.Context().Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	fmt.Println("PASS wt client bidi=both uni=both close=42 reason=done")
	return nil
}
