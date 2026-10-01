package tasktest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

func TestMemStore(t *testing.T) {
	RunStoreSuite(t, func(*testing.T) Store { return NewMemStore() })
}

func TestMemStoreErr(t *testing.T) {
	ctx, boom := context.Background(), errors.New("boom")
	s := NewMemStore()
	s.Err = boom

	calls := map[string]func() error{
		"Create":         func() error { _, err := s.Create(ctx, task.NewTask{Title: "a"}); return err },
		"Get":            func() error { _, err := s.Get(ctx, 1); return err },
		"List":           func() error { _, _, err := s.List(ctx, task.Filter{}); return err },
		"Update":         func() error { _, err := s.Update(ctx, 1, task.Patch{Done: ptr(true)}); return err },
		"Delete":         func() error { return s.Delete(ctx, 1) },
		"DueForReminder": func() error { _, err := s.DueForReminder(ctx, time.Now()); return err },
		"MarkReminded":   func() error { return s.MarkReminded(ctx, 1, time.Now()) },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, boom) {
			t.Errorf("%s error = %v, want %v", name, err, boom)
		}
	}
}
