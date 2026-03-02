package channels

import "testing"

func TestNewServiceWithStartGuideNumberUsesProvidedStart(t *testing.T) {
	svc := NewServiceWithStartGuideNumber(nil, 275)
	if svc.startGuideNumber != 275 {
		t.Fatalf("startGuideNumber = %d, want 275", svc.startGuideNumber)
	}
}

func TestNewServiceWithStartGuideNumberFallsBackToDefaultForInvalidStart(t *testing.T) {
	testCases := []int{0, -1, DynamicGuideStart}
	for _, start := range testCases {
		svc := NewServiceWithStartGuideNumber(nil, start)
		if svc.startGuideNumber != TraditionalGuideStart {
			t.Fatalf("start=%d produced %d, want %d", start, svc.startGuideNumber, TraditionalGuideStart)
		}
	}
}
