package task

import (
	"errors"
	"testing"
)

func TestNewTaskNormalize(t *testing.T) {
	tests := []struct {
		name    string
		title   string
		want    string
		wantErr error
	}{
		{name: "обычный заголовок", title: "купить молоко", want: "купить молоко"},
		{name: "пробелы обрезаются", title: "\t позвонить \n", want: "позвонить"},
		{name: "пустой", title: "", wantErr: ErrEmptyTitle},
		{name: "только пробелы", title: "   ", wantErr: ErrEmptyTitle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTask{Title: tt.title}.Normalize()
			if !errors.Is(err, tt.wantErr) || got.Title != tt.want {
				t.Errorf("Normalize(%q) = %q, %v; want %q, %v", tt.title, got.Title, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestPatchNormalize(t *testing.T) {
	title, blank, done := " новый ", "  ", true
	tests := []struct {
		name      string
		patch     Patch
		wantTitle string
		wantErr   error
	}{
		{name: "заголовок обрезается", patch: Patch{Title: &title}, wantTitle: "новый"},
		{name: "только done", patch: Patch{Done: &done}},
		{name: "пустой патч", patch: Patch{}, wantErr: ErrEmptyPatch},
		{name: "пустой заголовок", patch: Patch{Title: &blank}, wantErr: ErrEmptyTitle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.patch.Normalize()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Normalize() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantTitle != "" && (got.Title == nil || *got.Title != tt.wantTitle) {
				t.Errorf("Normalize().Title = %v, want %q", got.Title, tt.wantTitle)
			}
		})
	}
	if title != " новый " {
		t.Error("Normalize изменил строку вызывающего кода")
	}
}
