package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseFirmwareMix(t *testing.T) {
	mix, err := ParseFirmwareMix("1.4.2=70, 1.5.0=30")
	if err != nil || len(mix) != 2 || mix[0] != (FirmwareShare{"1.4.2", 70}) || mix[1] != (FirmwareShare{"1.5.0", 30}) {
		t.Fatalf("mix=%v err=%v", mix, err)
	}
	if mix, err := ParseFirmwareMix(""); err != nil || mix != nil {
		t.Fatalf("empty: mix=%v err=%v", mix, err)
	}
	for _, bad := range []string{"1.4.2", "1.4.2=0", "=3", "1.4.2=x"} {
		if _, err := ParseFirmwareMix(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestFirmwareMixIsProportionalAndStable(t *testing.T) {
	mix := []FirmwareShare{{"1.4.2", 70}, {"1.5.0", 30}}
	counts := map[string]int{}
	for i := 0; i < 200; i++ {
		v := firmwareFor(i, mix, "x")
		if v != firmwareFor(i, mix, "x") {
			t.Fatal("a device's firmware changed between calls")
		}
		counts[v]++
	}
	if counts["1.4.2"] != 140 || counts["1.5.0"] != 60 {
		t.Fatalf("counts = %v, want 140 and 60", counts)
	}
	if got := firmwareFor(3, nil, "1.4.2"); got != "1.4.2" {
		t.Fatalf("no mix: got %q", got)
	}
}

// A chunk on a thin link takes longer than a telemetry request is allowed.
func TestBundleTransfersGetTheLongerTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "key", 100*time.Millisecond)
	c.SetBulkTimeout(5 * time.Second)
	if err := c.UploadChunk(context.Background(), "d1", "tok", "b1", 0, []byte("data")); err != nil {
		t.Fatalf("chunk upload timed out on the short timeout: %v", err)
	}
	if err := c.Heartbeat(context.Background(), "d1", "tok"); err == nil {
		t.Fatal("an ordinary request should still time out quickly")
	}
	c.SetBulkTimeout(time.Millisecond) // never below the ordinary timeout
	if c.bulk.Timeout != 100*time.Millisecond {
		t.Fatalf("bulk timeout = %v", c.bulk.Timeout)
	}
}
