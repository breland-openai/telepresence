package cli

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestProcWaitTimeout(t *testing.T) {
	if os.Getenv("TP_TEST_WAIT_CHILD") == "1" {
		time.Sleep(time.Minute)
		return
	}
	tp := &TP{Exe: os.Args[0], Env: append(os.Environ(), "TP_TEST_WAIT_CHILD=1")}
	proc, err := tp.Start(t.Context(), "-test.run=^TestProcWaitTimeout$")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = proc.Wait(100 * time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "did not exit within") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}
