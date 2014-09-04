package infrastructure

import (
  "testing"
  "time"
  "math"
)

func TestGraphiteDateFormat(t *testing.T) {
  mytime := time.Date(2009, time.November, 10, 9, 0, 0, 0, time.Local)
  f := graphiteDateFormat(mytime)
  if f != "09:00_20091110" {
    t.Error(f)
  }
}

func TestParsingFloatGraphiteResult(t *testing.T) {
  t.Parallel()

  s := `[{"target": "machine.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185.0, 1409763000], [741.0, 1409790300], [null, 1409790600], [756.0, 1409790900]]}]`

  response := parseGraphiteResponse([]byte(s))

  // Introspection testing. Should be avoided if possible.
  if response.err != nil {
    t.Error("Response should not have had any errors.")
  }
  if l := len(response.points);l != 4 {
    t.Fatal("Not enough points:", l)
  }

  idps, err := response.AsInts()
  if err == nil {
    t.Error("Expected an error.")
  }
  if idps != nil {
    t.Error("Expected nil result.")
  }

  fpts, err := parseGraphiteResponse([]byte(s)).AsFloats()
  if err != nil {
    t.Error("Unexpected error.")
  }
  if fpts == nil {
    t.Fatal("Unexpected nil result.")
  }

  if len(fpts) != 4 {
    t.Fatal("Missing points:", len(fpts))
  }
  times := []int64{
    1409763000,
    1409790300,
    1409790600,
    1409790900,
  }
  values := []*float64{
    makeFloat64Pointer(185.0),
    makeFloat64Pointer(741.0),
    nil,
    makeFloat64Pointer(756.0),
  }
  for i, p := range fpts {
    if p.Time.Unix() != times[i] {
      t.Error("Incorrect UNIX timestamp. Expected:", times[i], "Got:", p.Time.Unix())
    }
    if (p.Value == nil && values[i] != nil) || (p.Value != nil && values[i] == nil) {
      t.Error("nil value mismatch for element:", i)
    } else if p.Value != nil && math.Abs(float64(*p.Value - *values[i])) > 0.0001 {
      t.Error("value mismatch. Got:", *p.Value, "Expected:", *values[i])
    }
  }
}

func TestParsingIntGraphiteResult(t *testing.T) {
  t.Parallel()

  s := `[{"target": "machine.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185, 1409763000], [741, 1409790300], [null, 1409790600], [756, 1409790900]]}]`

  response := parseGraphiteResponse([]byte(s))

  // Introspection testing. Should be avoided if possible.
  if response.err != nil {
    t.Error("Response should not have had any errors.")
  }
  if l := len(response.points);l != 4 {
    t.Fatal("Not enough points:", l)
  }

  // Floats are tested in separate test. Just making sure we aren't getting any
  // suspicious errors.
  idps, err := response.AsFloats()
  if err != nil {
    t.Error("Unexpected error.")
  }
  if idps == nil {
    t.Error("Unexpected nil result.")
  }

  fpts, err := parseGraphiteResponse([]byte(s)).AsInts()
  if err != nil {
    t.Error("Unexpected error.")
  }
  if fpts == nil {
    t.Fatal("Unexpected nil result.")
  }

  if len(fpts) != 4 {
    t.Fatal("Missing points:", len(fpts))
  }
  times := []int64{
    1409763000,
    1409790300,
    1409790600,
    1409790900,
  }
  values := []*int64{
    makeInt64Pointer(185),
    makeInt64Pointer(741),
    nil,
    makeInt64Pointer(756),
  }
  for i, p := range fpts {
    if p.Time.Unix() != times[i] {
      t.Error("Incorrect UNIX timestamp. Expected:", times[i], "Got:", p.Time.Unix())
    }
    if (p.Value == nil && values[i] != nil) || (p.Value != nil && values[i] == nil) {
      t.Error("nil value mismatch for element:", i)
    } else if p.Value != nil && *p.Value != *values[i] {
      t.Error("value mismatch. Got:", *p.Value, "Expected:", *values[i])
    }
  }
}

func makeFloat64Pointer(v float64) *float64 {
  r := new(float64)
  *r = v
  return r
}

func makeInt64Pointer(v int64) *int64 {
  r := new(int64)
  *r = v
  return r
}
