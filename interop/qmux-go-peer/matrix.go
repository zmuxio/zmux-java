package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/debug"
)

type matrixRequest struct {
	Op        string              `json:"op"`
	Version   int                 `json:"version"`
	Carrier   string              `json:"carrier"`
	Scenario  string              `json:"scenario"`
	Role      string              `json:"role"`
	Addresses map[string]string   `json:"addresses"`
	Payloads  map[string][]string `json:"payloads"`
}

func matrix() error {
	input, output := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	var req matrixRequest
	if err := input.Decode(&req); err != nil {
		return err
	}
	if req.Version != 1 {
		return fmt.Errorf("unsupported matrix version %d", req.Version)
	}
	if req.Op == "probe" {
		return matrixProbe(output)
	}
	if req.Op != "pair" || req.Carrier != "tcp" || req.Scenario != "plain-qmux" {
		return errors.New("expected a TCP plain-qmux matrix pair")
	}
	server := req.Role == "server"
	if !server && req.Role != "client" {
		return fmt.Errorf("unknown role %q", req.Role)
	}
	conn, err := matrixConn(req, output, server)
	if err != nil {
		return err
	}
	defer conn.Close()
	session, err := openSession(context.Background(), conn, server, configWithDatagrams())
	if err != nil {
		return err
	}
	defer session.Close()
	if err := output.Encode(map[string]any{"type": "attached", "confirmed_pipes": []int{0}}); err != nil {
		return err
	}
	var barrier struct{ Type string }
	if err := input.Decode(&barrier); err != nil {
		return err
	}
	if barrier.Type != "exercise" {
		return errors.New("missing exercise barrier")
	}
	sent := 0
	for _, wire := range req.Payloads[req.Role] {
		payload, err := hex.DecodeString(wire)
		if err != nil {
			return err
		}
		// This QMux implementation synchronously writes one record per SendDatagram.
		if err := session.SendDatagram(payload); err != nil {
			return err
		}
		sent++
	}
	other := "server"
	if server {
		other = "client"
	}
	received := make([]string, 0, len(req.Payloads[other]))
	for range req.Payloads[other] {
		payload, err := session.ReceiveDatagram(session.Context())
		if err != nil {
			return err
		}
		received = append(received, hex.EncodeToString(payload))
	}
	return output.Encode(map[string]any{
		"type": "result", "carrier": "tcp", "events": []string{"plain-qmux"},
		"tx": matrixCounts(sent), "rx": matrixCounts(len(received)), "received": received,
	})
}

func matrixConn(req matrixRequest, output *json.Encoder, server bool) (net.Conn, error) {
	if !server {
		return net.Dial("tcp", req.Addresses["tcp"])
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	if err := output.Encode(map[string]any{
		"type": "ready", "addresses": map[string]string{"tcp": listener.Addr().String()},
	}); err != nil {
		return nil, err
	}
	return listener.Accept()
}

func matrixCounts(records int) map[string]map[string]int {
	return map[string]map[string]int{"0": {"record": records, "native": 0, "capsule": 0}}
}

func matrixProbe(output *json.Encoder) error {
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("missing QMux peer build info")
	}
	var commit string
	for _, setting := range build.Settings {
		if setting.Key == "vcs.revision" {
			commit = setting.Value
		}
	}
	if commit == "" {
		return errors.New("missing QMux peer revision; build with -buildvcs=true")
	}
	modules := make(map[string]any, len(build.Deps))
	for _, dependency := range build.Deps {
		modules[dependency.Path] = dependency
	}
	return output.Encode(map[string]any{
		"type": "result", "commit": commit, "graph": map[string]any{"modules": modules},
		"capabilities": map[string]bool{"tcp": true},
	})
}
