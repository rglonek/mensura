package ingest

import "testing"

func TestReceiveRefusesAPublicBind(t *testing.T) {
	err := CheckReceivePosture(ReceiveOptions{TCPAddr: ":9640", UDPAddr: ":9640"})
	if err == nil {
		t.Fatal("an all-interfaces receive bind was accepted")
	}
	if err := CheckReceivePosture(ReceiveOptions{TCPAddr: "127.0.0.1:9640"}); err != nil {
		t.Fatalf("loopback tcp was refused: %v", err)
	}
	if err := CheckReceivePosture(ReceiveOptions{UDPAddr: "10.0.0.1:9640", AllowedSources: []string{"10.1.1.1"}}); err == nil {
		t.Fatal("a public UDP listener was accepted")
	}
	err = CheckReceivePosture(ReceiveOptions{
		HTTPAddr: "10.0.0.1:9640", AllowedSources: []string{"10.1.1.1"},
		TLSCert: "cert.pem", TLSKey: "key.pem", Token: "secret",
	})
	if err != nil {
		t.Fatalf("a TLS HTTP listener with a token and an allow list was refused: %v", err)
	}
}
