package main

// The secure carrier matrix shared by every implementation's peer:
// "matrix-server <scenario>" and "matrix-client <scenario> <addr> [tls-addr]
// [proof-key-id]". The scenarios, fixed keys and output lines are those of
// zmux-spec/interop/seal-matrix/matrix.py.

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

const (
	closeCode   = 85
	closeReason = "matrix-close"
)

var allProfiles = []proto.Profile{proto.ProfileEstablishPSK, proto.ProfileEstablishPSKAES, proto.ProfileEstablish, proto.ProfileEstablishAES}

func scenarioProfile(sc string) proto.Profile {
	switch sc {
	case "xx", "appdata", "proof":
		return proto.ProfileEstablish
	}
	return proto.ProfileEstablishPSK
}

func multipath(sc string) bool { return sc == "join" || sc == "epoch" }

func sessionConfig(sc string) *zmux.Config {
	c := &zmux.Config{}
	if multipath(sc) {
		c.MaxPipes = 4
		c.AllowRecordMode = func(zmux.RecordPolicyInput, zmux.RecordMode, any) bool { return true }
	}
	return c
}

func mustKey(b byte) *ecdh.PrivateKey {
	k, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{b}, 32))
	if err != nil {
		fail(err)
	}
	return k
}

func matrixKeys() (handle, psk []byte) {
	h, p, err := seal.DeriveHandlePSK([]byte("zmux interop pre-shared key 0123"), []byte("interop"), nil)
	if err != nil {
		fail(err)
	}
	return h, p
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

func failf(format string, a ...any) {
	fmt.Printf("FAIL "+format+"\n", a...)
	os.Exit(1)
}

// matrixGate resolves the one Handle and keeps the client's Peer as the
// session's host value.
type matrixGate struct{ handle, psk []byte }

func (g *matrixGate) Resolve(_ context.Context, h []byte, _ netip.AddrPort) ([]byte, error) {
	if !bytes.Equal(h, g.handle) {
		return nil, errors.New("unknown handle")
	}
	return g.psk, nil
}
func (g *matrixGate) Credential(context.Context, seal.CredentialRequest) (*proto.Credential, error) {
	return nil, nil
}
func (g *matrixGate) Commit(_ context.Context, a seal.CommitRequest) (any, error) { return a.Peer, nil }

func printAppData(d []seal.AppData) {
	for _, e := range d {
		fmt.Printf("APPDATA %d %s fs=%t auth=%t\n", e.Message, e.Data, e.ForwardSecret, e.Authenticated)
	}
}

func selfSigned() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fail(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		fail(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// listenAddr is the server's listen address from the environment variable
// env (for runs across hosts), by default an ephemeral loopback port.
func listenAddr(env string) string {
	if a := os.Getenv(env); a != "" {
		return a
	}
	return "127.0.0.1:0"
}

func matrixServer(sc string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	handle, psk := matrixKeys()
	sc2 := &seal.ServerConfig{Gate: &matrixGate{handle, psk}, Key: mustKey(0x5a), AcceptedProfiles: allProfiles}
	switch sc {
	case "appdata":
		sc2.ClientAnchor = seal.PinAnchor(seal.NodeKeyID(mustKey(0x6b).PublicKey().Bytes()))
		sc2.AppData = func(_ context.Context, r seal.AppDataRequest) ([]byte, error) {
			if r.Message == 2 {
				return []byte("srv-msg2"), nil
			}
			return nil, nil
		}
	case "proof":
		k, err := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
		if err != nil {
			fail(err)
		}
		id, err := seal.KeyID(k.Alg(), k.Public())
		if err != nil {
			fail(err)
		}
		sc2.Signer = seal.KeySigner(k)
		fmt.Printf("PROOFKEY %x\n", id[:])
	}
	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: sessionConfig(sc), WrapPipe: sc2})
	if err != nil {
		fail(err)
	}
	ln, err := net.Listen("tcp", listenAddr("ZMUX_MATRIX_LISTEN"))
	if err != nil {
		fail(err)
	}
	go func() { _ = l.Serve(ctx, ln) }()
	fmt.Println("LISTEN " + ln.Addr().String())
	if sc == "join" {
		tl, err := tls.Listen("tcp", listenAddr("ZMUX_MATRIX_LISTENTLS"), &tls.Config{Certificates: []tls.Certificate{selfSigned()}, MinVersion: tls.VersionTLS13})
		if err != nil {
			fail(err)
		}
		go func() {
			for {
				nc, err := tl.Accept()
				if err != nil {
					return
				}
				go func() { _ = l.HandlePipe(ctx, zmux.SecureTLS(nc.(*tls.Conn)), zmux.PipeHint{}) }()
			}
		}()
		fmt.Println("LISTENTLS " + tl.Addr().String())
	}
	conn, err := l.Accept(ctx)
	if err != nil {
		failf("accept: %v", err)
	}
	peer, _ := zmux.AdmittedAs[*seal.Peer](conn)
	if peer == nil {
		failf("no Peer as the session's host value")
	}
	if sc == "appdata" {
		printAppData(peer.AppData)
	}
	switch sc {
	case "close":
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			failf("accept stream: %v", err)
		}
		if _, err := io.ReadFull(st, make([]byte, 64<<10)); err != nil {
			failf("read: %v", err)
		}
		conn.Abort(closeCode, closeReason)
		_ = conn.Wait()
		fmt.Println("CLOSING")
		fmt.Println("OK")
		os.Exit(0)
	}
	serverExchange(ctx, conn)
	if sc == "close-idle" {
		conn.Abort(closeCode, closeReason)
		_ = conn.Wait()
		fmt.Println("CLOSING")
		fmt.Println("OK")
		os.Exit(0)
	}
	_ = conn.Wait()
	if sc == "epoch" {
		fmt.Printf("EPOCH %d\n", conn.Epoch())
		if conn.Epoch() < 1 {
			failf("epoch %d", conn.Epoch())
		}
	}
	_ = l.Close()
	fmt.Println("OK")
}

func serverExchange(ctx context.Context, conn *zmux.Conn) {
	a, err := conn.AcceptStream(ctx)
	if err != nil {
		failf("accept A: %v", err)
	}
	b, err := io.ReadAll(a)
	if err != nil {
		failf("read A: %v", err)
	}
	if _, err := a.Write(b); err != nil {
		failf("echo A: %v", err)
	}
	_ = a.Close()
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		failf("open B: %v", err)
	}
	want := pattern(64 << 10)
	go func() { _, _ = s.Write(want); _ = s.Close() }()
	got, err := io.ReadAll(s)
	if err != nil {
		failf("read B: %v", err)
	}
	if !bytes.Equal(got, want) {
		failf("B echo mismatch (%d bytes)", len(got))
	}
}

func clientExchange(ctx context.Context, conn *zmux.Conn) {
	a, err := conn.OpenStreamSync(ctx)
	if err != nil {
		failf("open A: %v", err)
	}
	want := pattern(1 << 20)
	go func() { _, _ = a.Write(want); _ = a.Close() }()
	got, err := io.ReadAll(a)
	if err != nil {
		failf("read A: %v", err)
	}
	if !bytes.Equal(got, want) {
		failf("A echo mismatch (%d bytes)", len(got))
	}
	b, err := conn.AcceptStream(ctx)
	if err != nil {
		failf("accept B: %v", err)
	}
	d, err := io.ReadAll(b)
	if err != nil {
		failf("read B: %v", err)
	}
	if _, err := b.Write(d); err != nil {
		failf("echo B: %v", err)
	}
	_ = b.Close()
}

// reportClose waits for the session's end and says whether the peer's
// CONNECTION_CLOSE arrived.
func reportClose(conn *zmux.Conn) {
	err := conn.Wait()
	var ae *zmux.ApplicationError
	if errors.As(err, &ae) && ae.Remote {
		fmt.Printf("CLOSE code=%d reason=%s\n", ae.ErrorCode, ae.ErrorMessage)
		if ae.ErrorCode == closeCode && ae.ErrorMessage == closeReason {
			fmt.Println("OK")
			os.Exit(0)
		}
		os.Exit(1)
	}
	fmt.Printf("CLOSE lost %v\n", err)
	os.Exit(3)
}

func matrixClient(sc string, args []string) {
	if len(args) < 1 {
		fail(errors.New("matrix-client <scenario> <addr> [tls-addr] [proof-key-id]"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	addr := args[0]
	node := mustKey(0x5a)
	nodeID := seal.NodeKeyID(node.PublicKey().Bytes())
	profile := scenarioProfile(sc)
	cc := &seal.ClientConfig{Anchor: seal.PinAnchor(nodeID), AllowedProfiles: []proto.Profile{profile}}
	if profile.HasPSK() {
		cc.Handle, cc.PSK = matrixKeys()
	}
	switch sc {
	case "appdata":
		cc.Key = mustKey(0x6b)
		cc.AppData = func(_ context.Context, r seal.AppDataRequest) ([]byte, error) {
			if r.Message == 3 {
				return []byte("cli-msg3"), nil
			}
			return nil, nil
		}
	case "proof":
		if len(args) < 2 {
			fail(errors.New("proof needs the proof key id"))
		}
		raw, err := hex.DecodeString(args[1])
		if err != nil || len(raw) != len(proto.KeyID{}) {
			fail(fmt.Errorf("proof key id %q", args[1]))
		}
		var id proto.KeyID
		copy(id[:], raw)
		cc.Anchor = seal.PinAnchor(nodeID, id)
		cc.RequirePQAuth = true
	}
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		fail(err)
	}
	p, err := seal.Establish(ctx, zmux.NetPipe(nc), cc)
	if err != nil {
		failf("establish: %v", err)
	}
	peer, _ := seal.PeerOf(p)
	conn, err := zmux.Start(ctx, p, zmux.RoleClient, sessionConfig(sc))
	if err != nil {
		failf("start: %v", err)
	}
	switch sc {
	case "appdata":
		printAppData(peer.AppData)
	case "proof":
		fmt.Printf("PQ confidential=%t authenticated=%t\n", peer.PQConfidential, peer.PQAuthenticated)
		if !peer.PQAuthenticated {
			failf("not PQ authenticated")
		}
	case "join":
		if len(args) < 2 {
			fail(errors.New("join needs the TLS address"))
		}
		tc, err := tls.Dial("tcp", args[1], &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
		if err != nil {
			failf("tls dial: %v", err)
		}
		bound := zmux.RecordBound
		if _, err := conn.AddPipe(ctx, zmux.SecureTLS(tc), zmux.PipeOptions{RecordMode: zmux.Set(bound)}); err != nil {
			failf("bound join: %v", err)
		}
		fmt.Printf("PIPES %d\n", len(conn.PipeStats()))
	case "epoch":
		if err := conn.AdvanceEpoch(); err != nil {
			failf("advance epoch: %v", err)
		}
		for {
			ev, err := conn.NextEvent(ctx)
			if err != nil {
				failf("no EpochCommitted: %v", err)
			}
			if ev.Kind == zmux.EventEpochCommitted && ev.Epoch >= 1 {
				break
			}
		}
		nc2, err := net.Dial("tcp", addr)
		if err != nil {
			fail(err)
		}
		if _, err := conn.AddPipe(ctx, zmux.NetPipe(nc2), zmux.PipeOptions{}); err != nil {
			failf("join after epoch: %v", err)
		}
	case "close":
		st, err := conn.OpenStreamSync(ctx)
		if err != nil {
			failf("open: %v", err)
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			chunk := pattern(16 << 10)
			for {
				if _, err := st.Write(chunk); err != nil {
					return
				}
			}
		})
		reportClose(conn)
	}
	clientExchange(ctx, conn)
	if sc == "close-idle" {
		reportClose(conn)
	}
	if sc == "epoch" {
		fmt.Printf("EPOCH %d\n", conn.Epoch())
		if conn.Epoch() < 1 {
			failf("epoch %d", conn.Epoch())
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := conn.Shutdown(sctx, 0, ""); err != nil {
		failf("shutdown: %v", err)
	}
	fmt.Println("OK")
}
