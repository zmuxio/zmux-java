// Command qmux-go-peer is a scripted plain QMux peer built on qmux-go, for
// interoperability tests over TCP. It speaks the command protocol of the
// zmux-rs interop/qmux-peer helper; see README.md.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/okdaichi/qmux-go/qmux"
	"github.com/quic-go/quic-go"
)

const maxCommand = 1024

func usage() {
	fmt.Fprintln(os.Stderr, "usage: qmux-go-peer server|client <addr> [--rtt-ms <ms>] [--no-datagrams]")
	os.Exit(2)
}

func main() {
	args := os.Args[1:]
	if len(args) < 2 {
		usage()
	}
	server := args[0] == "server"
	if !server && args[0] != "client" {
		usage()
	}
	config := &qmux.Config{EnableDatagrams: true, MaxDatagramFrameSize: 16382, KeepAlivePeriod: time.Second}
	for i := 2; i < len(args); i++ {
		switch {
		case args[i] == "--rtt-ms" && i+1 < len(args):
			ms, err := strconv.Atoi(args[i+1])
			if err != nil {
				usage()
			}
			config.KeepAlivePeriod = time.Duration(ms) * time.Millisecond
			i++
		case args[i] == "--no-datagrams":
			config.EnableDatagrams = false
		default:
			usage()
		}
	}
	var conn net.Conn
	var err error
	if server {
		ln, lerr := net.Listen("tcp", args[1])
		if lerr != nil {
			panic(lerr)
		}
		fmt.Printf("LISTEN %s\n", ln.Addr())
		conn, err = ln.Accept()
	} else {
		conn, err = net.Dial("tcp", args[1])
	}
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var session *qmux.Conn
	if server {
		session, err = qmux.Server(ctx, conn, config)
	} else {
		session, err = qmux.Dial(ctx, conn, config)
	}
	cancel()
	if err != nil {
		fmt.Printf("CLOSED handshake %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ESTABLISHED")
	go acceptBidi(session)
	go acceptUni(session)
	if config.EnableDatagrams {
		go echoDatagrams(session)
	}
	<-session.Context().Done()
	fmt.Printf("RTT %s\n", rtt(session))
	fmt.Printf("CLOSED %s\n", describe(context.Cause(session.Context())))
}

func rtt(s *qmux.Conn) string {
	if d := s.ConnectionStats().SmoothedRTT; d > 0 {
		return strconv.FormatInt(d.Microseconds(), 10)
	}
	return "none"
}

func describe(err error) string {
	var app *quic.ApplicationError
	if errors.As(err, &app) {
		return fmt.Sprintf("app %d %s", uint64(app.ErrorCode), app.ErrorMessage)
	}
	var tr *quic.TransportError
	if errors.As(err, &tr) {
		return fmt.Sprintf("transport %d %s", uint64(tr.ErrorCode), tr.ErrorMessage)
	}
	return fmt.Sprintf("other %v", err)
}

// streamCode is the full code of a stream reset or stop.
func streamCode(err error) string {
	var se *quic.StreamError
	if errors.As(err, &se) {
		return strconv.FormatUint(uint64(se.ErrorCode), 10)
	}
	return fmt.Sprintf("error(%v)", err)
}

// report sends text on a new unidirectional stream.
func report(s *qmux.Conn, text string) {
	u, err := s.OpenUniStreamSync(s.Context())
	if err != nil {
		return
	}
	u.Write([]byte(text))
	u.Close()
}

func reply(st *qmux.Stream, text string) {
	st.Write([]byte(text))
	st.Close()
}

func acceptBidi(s *qmux.Conn) {
	for {
		st, err := s.AcceptStream(s.Context())
		if err != nil {
			return
		}
		go bidi(s, st)
	}
}

func acceptUni(s *qmux.Conn) {
	for {
		r, err := s.AcceptUniStream(s.Context())
		if err != nil {
			return
		}
		go func() {
			data, err := io.ReadAll(r)
			if err != nil {
				report(s, "reset:"+streamCode(err))
				return
			}
			report(s, string(data))
		}()
	}
}

func echoDatagrams(s *qmux.Conn) {
	for {
		d, err := s.ReceiveDatagram(s.Context())
		if err != nil {
			return
		}
		s.SendDatagram(d)
	}
}

// drain reads to the end; a reset is reported.
func drain(s *qmux.Conn, st *qmux.Stream) bool {
	if _, err := io.Copy(io.Discard, st); err != nil {
		report(s, "reset:"+streamCode(err))
		return false
	}
	return true
}

func bidi(s *qmux.Conn, st *qmux.Stream) {
	var acc []byte
	buf := make([]byte, 64*1024)
	fin := false
	newline := -1
	for newline < 0 && len(acc) <= maxCommand && !fin {
		n, err := st.Read(buf)
		acc = append(acc, buf[:n]...)
		newline = bytes.IndexByte(acc, '\n')
		if err == io.EOF {
			fin = true
		} else if err != nil {
			report(s, "reset:"+streamCode(err))
			return
		}
	}
	command, rest := string(acc), []byte(nil)
	if newline >= 0 {
		command, rest = string(acc[:newline]), acc[newline+1:]
	}
	verb, arg, _ := strings.Cut(command, ":")
	switch verb {
	case "echo":
		if _, err := st.Write(rest); err != nil {
			return
		}
		if !fin {
			if _, err := io.Copy(st, st); err != nil {
				report(s, "reset:"+streamCode(err))
				return
			}
		}
		st.Close()
		return
	case "count":
		n := int64(len(rest))
		if !fin {
			m, err := io.Copy(io.Discard, st)
			n += m
			if err != nil {
				report(s, fmt.Sprintf("reset:%s:%d", streamCode(err), n))
				return
			}
		}
		reply(st, fmt.Sprintf("count=%d", n))
		return
	case "stop":
		code, _ := strconv.ParseUint(arg, 10, 64)
		st.CancelRead(quic.StreamErrorCode(code))
		reply(st, "stopped")
		return
	}
	if !fin && !drain(s, st) {
		return
	}
	switch verb {
	case "flood":
		chunk := bytes.Repeat([]byte{'x'}, 1024)
		var err error
		for err == nil {
			_, err = st.Write(chunk)
			time.Sleep(time.Millisecond)
		}
		report(s, "stopped:"+streamCode(err))
	case "reset":
		code, _ := strconv.ParseUint(arg, 10, 64)
		st.Write(bytes.Repeat([]byte{7}, 1000))
		time.Sleep(20 * time.Millisecond)
		st.CancelWrite(quic.StreamErrorCode(code))
	case "open-bidi":
		o, err := s.OpenStreamSync(s.Context())
		if err != nil {
			reply(st, "opened-failed")
			return
		}
		o.Write([]byte(arg))
		o.Close()
		answer, err := io.ReadAll(o)
		if err != nil {
			reply(st, "opened-failed")
			return
		}
		reply(st, "opened="+string(answer))
	case "open-uni":
		report(s, arg)
		reply(st, "sent")
	case "rtt":
		reply(st, "rtt="+rtt(s))
	case "datagram-size":
		// qmux-go does not report its maximum payload; it follows the peer's limit.
		reply(st, "max=unknown")
	case "close":
		codeText, reason, _ := strings.Cut(arg, ":")
		code, _ := strconv.ParseUint(codeText, 10, 64)
		reply(st, "closing")
		time.Sleep(50 * time.Millisecond)
		s.CloseWithError(qmux.ApplicationErrorCode(code), reason)
	default:
		reply(st, "unknown="+command)
	}
}
