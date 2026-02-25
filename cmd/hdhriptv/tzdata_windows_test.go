//go:build windows

package main

import (
	"testing"
	"time"
)

func TestWindowsBuildLoadsSchedulerDefaultTimezone(t *testing.T) {
	location, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatalf("time.LoadLocation(America/Chicago) error = %v", err)
	}
	if location == nil {
		t.Fatal("time.LoadLocation(America/Chicago) location = nil")
	}
}
