// A scripted Establish peer for interop: "server PROFILE" prints "LISTEN addr" and
// serves one carrier; "client PROFILE addr" dials. Both print "ESTABLISHED <profile>"
// and then the server echoes one stream, the client sends one and checks it.
// "matrix-server" and "matrix-client" run the scenarios of matrix.go.
package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

type gate struct{ handle, psk []byte }

func (g *gate) Resolve(_ context.Context, h []byte, _ netip.AddrPort) ([]byte, error) {
	if !bytes.Equal(h, g.handle) {
		return nil, errors.New("unknown handle")
	}
	return g.psk, nil
}
func (g *gate) Credential(context.Context, seal.CredentialRequest) (*proto.Credential, error) {
	return nil, nil
}
func (g *gate) Commit(context.Context, seal.CommitRequest) (any, error) { return "ok", nil }

func fail(err error) {
	fmt.Fprintln(os.Stderr, "sealpeer:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 3 {
		fail(errors.New("server PROFILE | client PROFILE addr | matrix-server SCENARIO | matrix-client SCENARIO addr..."))
	}
	switch os.Args[1] {
	case "matrix-server":
		matrixServer(os.Args[2])
		return
	case "matrix-client":
		matrixClient(os.Args[2], os.Args[3:])
		return
	}
	n, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fail(err)
	}
	profile := proto.Profile(n)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	handle, psk, err := seal.DeriveHandlePSK(make([]byte, 32), []byte("test"), nil)
	if err != nil {
		fail(err)
	}
	node, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		fail(err)
	}
	switch os.Args[1] {
	case "server":
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fail(err)
		}
		fmt.Println("LISTEN " + ln.Addr().String())
		c, err := ln.Accept()
		if err != nil {
			fail(err)
		}
		_ = ln.Close()
		sc := &seal.ServerConfig{Gate: &gate{handle, psk}, Key: node, AcceptedProfiles: []proto.Profile{proto.ProfileEstablishPSK, proto.ProfileEstablishPSKAES, proto.ProfileEstablish, proto.ProfileEstablishAES}}
		p, err := sc.WrapPipe(ctx, zmux.NetPipe(c), zmux.PipeHint{})
		if err != nil {
			fail(err)
		}
		peer, _ := seal.PeerOf(p)
		conn, err := zmux.Start(ctx, p, zmux.RoleServer, &zmux.Config{})
		if err != nil {
			fail(err)
		}
		fmt.Printf("ESTABLISHED %d\n", peer.Profile)
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			fail(err)
		}
		b, err := io.ReadAll(st)
		if err != nil {
			fail(err)
		}
		if _, err := st.Write(b); err != nil {
			fail(err)
		}
		_ = st.Close()
		_ = conn.Wait()
		fmt.Println("CLOSED")
	case "client":
		c, err := net.Dial("tcp", os.Args[3])
		if err != nil {
			fail(err)
		}
		cc := &seal.ClientConfig{Anchor: seal.PinAnchor(seal.NodeKeyID(node.PublicKey().Bytes())), AllowedProfiles: []proto.Profile{profile}}
		if profile.HasPSK() {
			cc.Handle, cc.PSK = handle, psk
		}
		p, err := seal.Establish(ctx, zmux.NetPipe(c), cc)
		if err != nil {
			fail(err)
		}
		peer, _ := seal.PeerOf(p)
		conn, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{})
		if err != nil {
			fail(err)
		}
		fmt.Printf("ESTABLISHED %d\n", peer.Profile)
		st, err := conn.OpenStreamSync(ctx)
		if err != nil {
			fail(err)
		}
		msg := make([]byte, 1<<20)
		_, _ = rand.Read(msg)
		go func() { _, _ = st.Write(msg); _ = st.Close() }()
		got, err := io.ReadAll(st)
		if err != nil {
			fail(err)
		}
		if !bytes.Equal(got, msg) {
			fail(errors.New("echo mismatch"))
		}
		fmt.Println("OK")
		_ = conn.Close()
	default:
		fail(errors.New("unknown mode"))
	}
}
