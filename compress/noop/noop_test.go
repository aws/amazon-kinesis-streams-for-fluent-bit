package noop

import (
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	// Nothing to check, no panic is enough for this test.
	_ = New()
}

func TestNoop_Compress(t *testing.T) {
	want := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	z := Noop{}
	got, err := z.Compress([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	if err != nil {
		t.Errorf("Noop.Compress() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Noop.Compress() = %v, want %v", got, want)
	}
}
