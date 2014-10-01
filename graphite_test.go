package infrastructure

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGraphiteDateFormat(t *testing.T) {
	mytime := time.Date(2009, time.November, 10, 9, 0, 0, 0, time.Local)
	f := graphiteDateFormat(mytime)
	if f != "09:00_20091110" {
		t.Error(f)
	}
}

func TestIntegrationMulti(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[{"target": "machine.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185, 1409763000], [741, 1409790300], [null, 1409790600]]},{"target": "machine2.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185, 1409763000], [741, 1409790300]]}]`)
	}))
	defer ts.Close()

	c, err := New(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	pointlist, err := c.QueryMultiSince([]string{"machine*.jvm.gc.PS-MarkSweep.runs"}, time.Second*time.Duration(200))
	if err != nil {
		t.Fatal(err)
	}
	points := pointlist.asMap()

	if len(points) != 2 {
		t.Fatal("Missing points:", len(points))
	}
	if ints, _ := points["machine.jvm.gc.PS-MarkSweep.runs"].AsInts(); len(ints) != 3 {
		t.Error("Expected first points target to have length 3:", len(ints))
	}
	if ints, _ := points["machine2.jvm.gc.PS-MarkSweep.runs"].AsInts(); len(ints) != 2 {
		t.Error("Expected first points target to have length 2:", len(ints))
	}
}

func TestIntegration(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[{"target": "machine.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185, 1409763000], [741, 1409790300], [null, 1409790600], [756, 1409790900]]}]`)
	}))
	defer ts.Close()

	c, err := New(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	points, err := c.QueryIntsSince("machine.jvm.gc.PS-MarkSweep.runs", time.Second*time.Duration(200))
	if err != nil {
		t.Fatal(err)
	}

	if len(points) != 4 {
		t.Fatal("Missing points:", len(points))
	}
}

func TestParsingFloatGraphiteResult(t *testing.T) {
	t.Parallel()

	s := `[{"target": "machine.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185.0, 1409763000], [741.0, 1409790300], [null, 1409790600], [756.0, 1409790900]]}]`

	response, err := parseGraphiteResponse([]byte(s))
	if err != nil {
		t.Fatal(err)
	}

	idps, err := response[0].AsInts()
	if err == nil {
		t.Error("Expected an error.")
	}
	if idps != nil {
		t.Error("Expected nil result.")
	}
	if len(response) != 1 {
		t.Error("Unexpected list length:", len(response))
	}

	fpts, err := response[0].AsFloats()
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
		} else if p.Value != nil && math.Abs(float64(*p.Value-*values[i])) > 0.0001 {
			t.Error("value mismatch. Got:", *p.Value, "Expected:", *values[i])
		}
	}
}

func TestParsingIntGraphiteResult(t *testing.T) {
	t.Parallel()

	s := `[{"target": "machine.jvm.gc.PS-MarkSweep.runs", "datapoints": [[185, 1409763000], [741, 1409790300], [null, 1409790600], [756, 1409790900]]}]`

	response, err := parseGraphiteResponse([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	if len(response) != 1 {
		t.Fatal("Unexpected list length:", len(response))
	}

	// Introspection testing. Should be avoided if possible.
	if response[0].err != nil {
		t.Error("Response should not have had any errors.")
	}
	if l := len(response[0].points); l != 4 {
		t.Fatal("Not enough points:", l)
	}

	// Floats are tested in separate test. Just making sure we aren't getting any
	// suspicious errors.
	idps, err := response[0].AsFloats()
	if err != nil {
		t.Error("Unexpected error.")
	}
	if idps == nil {
		t.Error("Unexpected nil result.")
	}

	fpts, err := response[0].AsInts()
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
