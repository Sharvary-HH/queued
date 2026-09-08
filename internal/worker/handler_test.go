package worker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Sharvary-HH/queued/internal/worker"
)

func noop(context.Context, []byte) error { return nil }

func TestRegistryRegisterAndLookup(t *testing.T) {
	t.Parallel()
	reg := worker.NewRegistry()

	if err := reg.Register("email", noop); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Lookup("email"); err != nil {
		t.Fatalf("lookup of a registered kind: %v", err)
	}
}

// Registering the same kind twice is almost always two packages both thinking
// they own a name, and a silent overwrite makes which one wins depend on init
// order.
func TestRegistryRejectsDuplicates(t *testing.T) {
	t.Parallel()
	reg := worker.NewRegistry()

	if err := reg.Register("email", noop); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("email", noop); err == nil {
		t.Error("registering the same kind twice was allowed")
	}
}

func TestRegistryRejectsEmptyAndNil(t *testing.T) {
	t.Parallel()
	reg := worker.NewRegistry()

	if err := reg.Register("", noop); err == nil {
		t.Error("empty kind was allowed")
	}
	if err := reg.Register("email", nil); err == nil {
		t.Error("nil handler was allowed")
	}
}

func TestRegistryUnknownKind(t *testing.T) {
	t.Parallel()
	reg := worker.NewRegistry()

	_, err := reg.Lookup("nope")
	if !errors.Is(err, worker.ErrNoHandler) {
		t.Fatalf("got %v, want ErrNoHandler", err)
	}
	// Retryable on purpose: during a rolling deploy the old workers do not yet
	// know a kind the new code enqueues, and dead-lettering those would be a
	// far worse mistake than waiting a few minutes.
	if worker.IsPermanent(err) {
		t.Error("an unknown kind is permanent; a rolling deploy would dead-letter valid jobs")
	}
}

func TestRegistryKindsIsSorted(t *testing.T) {
	t.Parallel()
	reg := worker.NewRegistry()
	for _, k := range []string{"zeta", "alpha", "mu"} {
		reg.MustRegister(k, noop)
	}

	got := reg.Kinds()
	want := []string{"alpha", "mu", "zeta"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("Kinds() = %v, want %v", got, want)
	}
}

func TestPermanentWrapping(t *testing.T) {
	t.Parallel()

	inner := errors.New("payload is invalid")
	err := worker.Permanent(inner)

	if !worker.IsPermanent(err) {
		t.Error("wrapped error is not reported as permanent")
	}
	if !errors.Is(err, inner) {
		t.Error("wrapping lost the underlying error")
	}
	if err.Error() != inner.Error() {
		t.Errorf("message = %q, want %q", err.Error(), inner.Error())
	}

	// Still permanent after another layer, since handlers add context freely.
	wrapped := fmt.Errorf("processing order 17: %w", err)
	if !worker.IsPermanent(wrapped) {
		t.Error("permanence did not survive being wrapped again")
	}

	if worker.IsPermanent(inner) {
		t.Error("a plain error was reported as permanent")
	}
	if worker.Permanent(nil) != nil {
		t.Error("Permanent(nil) should be nil so `return Permanent(f())` works")
	}
}
