package service

import "testing"

func TestFormattedMetricNamesRemainDistinct(t *testing.T) {
	r := NewRegistry(nil).(*Registry)
	a, b := r.GetOrRegisterGauge("fixture/%s", "a"), r.GetOrRegisterGauge("fixture/%s", "b")
	if a == b {
		t.Fatal("distinct names alias")
	}
	if a != r.GetOrRegisterGauge("fixture/%s", "a") {
		t.Fatal("same name not reused")
	}
	if r.Get("fixture/a") != a || r.Get("fixture/b") != b {
		t.Fatal("registry contains format instead of name")
	}
}
