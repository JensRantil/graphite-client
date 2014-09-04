package infrastructure

import (
  "fmt"
  "time"
  httpurl "net/url"
  "errors"
  "encoding/json"
  "net/http"
  "io/ioutil"
  "bytes"
)

type Client struct{
  url *httpurl.URL
}

func New(url string) (*Client, error) {
  u, err := httpurl.Parse(url)
  if err != nil {
    return nil, err
  }
  return &Client{u}, nil
}

func NewFromURL(url httpurl.URL) *Client {
  return &Client{&url}
}

type TimeInterval struct {
  From time.Time
  To time.Time
}

func (t *TimeInterval) Check() error {
  if t.From.After(t.To) {
    return errors.New("From must be before To.")
  }
  return nil
}

func graphiteDateFormat(t time.Time) string {
  return fmt.Sprintf("%02d:%02d_%d%02d%02d", t.Hour(), t.Minute(), t.Year(), t.Month(), t.Day())
}

type FloatDatapoint struct {
  Time time.Time
  Value *float64
}

type IntDatapoint struct {
  Time time.Time
  Value *int64
}

type Datapoints struct {
  // Previous error to make API nicer.
  err error
  points [][]interface{}
}

func (d Datapoints) AsInts() ([]IntDatapoint, error) {
  if d.err != nil {
    return nil, d.err
  }

  points := make([]IntDatapoint, 0, len(d.points))
  for _, point := range d.points {
    jsonUnixTime, ok := point[1].(json.Number)
    if !ok {
      return nil, errors.New("Unix timestamp not number.")
    }
    unixTime, err := jsonUnixTime.Int64()
    if err != nil {
      return nil, errors.New("Unix time not proper number.")
    }

    var value *int64
    if point[0] != nil {
      jsonValue, ok := point[0].(json.Number)
      if !ok {
        return nil, errors.New("Value not a number.")
      }
      value = new(int64)
      *value, err = jsonValue.Int64()
      if err != nil {
        return nil, errors.New("Value not proper number.")
      }
    }
    points = append(points, IntDatapoint{time.Unix(unixTime, 0), value})
  }

  return points, nil
}

func (d Datapoints) AsFloats() ([]FloatDatapoint, error) {
  if d.err != nil {
    return nil, d.err
  }

  points := make([]FloatDatapoint, 0, len(d.points))
  for _, point := range d.points {
    jsonUnixTime, ok := point[1].(json.Number)
    if !ok {
      return nil, errors.New("Unix timestamp not number.")
    }
    unixTime, err := jsonUnixTime.Int64()
    if err != nil {
      return nil, errors.New("Unix time not proper number.")
    }

    var value *float64
    if point[0] != nil {
      jsonValue, ok := point[0].(json.Number)
      if !ok {
        return nil, errors.New("Value not a number.")
      }
      value = new(float64)
      *value, err = jsonValue.Float64()
      if err != nil {
        return nil, errors.New("Value not proper number.")
      }
    }
    points = append(points, FloatDatapoint{time.Unix(unixTime, 0), value})
  }

  return points, nil
}

func constructQueryPart(q string) httpurl.Values {
  query := make(httpurl.Values)
  query.Add("target", q)
  query.Add("format", "json")
  return query
}

// Helper method to make it easier to create an interface for Client.
func (g *Client) QueryInts(q string, interval TimeInterval) ([]IntDatapoint, error) {
  return g.Query(q, interval).AsInts()
}

// Helper method to make it easier to create an interface for Client.
func (g *Client) QueryFloats(q string, interval TimeInterval) ([]FloatDatapoint, error) {
  return g.Query(q, interval).AsFloats()
}

// Helper method to make it easier to create an interface for Client.
func (g *Client) QueryIntsSince(q string, ago time.Duration) ([]IntDatapoint, error) {
  return g.QuerySince(q, ago).AsInts()
}

// Helper method to make it easier to create an interface for Client.
func (g *Client) QueryFloatsSince(q string, ago time.Duration) ([]FloatDatapoint, error) {
  return g.QuerySince(q, ago).AsFloats()
}

// Fetches a Graphite result. Deferring identifying whether the result are ints
// of floats to later. Useful in clients that executes adhoc queries.
func (g *Client) Query(q string, interval TimeInterval) Datapoints {
  if err := interval.Check(); err != nil {
    return Datapoints{err, nil}
  }

  // Cloning to be able to modify.
  url := g.url

  queryPart := constructQueryPart(q)
  queryPart.Add("from", graphiteDateFormat(interval.From))
  queryPart.Add("until", graphiteDateFormat(interval.To))
  url.RawQuery = queryPart.Encode()

  resp, err := http.Get(url.String())
  if err != nil {
    return Datapoints{err, nil}
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return Datapoints{err, nil}
  }

  return parseGraphiteResponse(body)
}

func (g *Client) QuerySince(q string, ago time.Duration) Datapoints {
  if ago.Nanoseconds() > 0 {
    return Datapoints{errors.New("Duration is expected to be positive."), nil}
  }

  // Cloning to be able to modify.
  url := g.url

  queryPart := constructQueryPart(q)
  queryPart.Add("from", fmt.Sprintf("%dminutes", ago.Minutes()))
  url.RawQuery = queryPart.Encode()

  resp, err := http.Get(url.String())
  if err != nil {
    return Datapoints{err, nil}
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return Datapoints{err, nil}
  }

  return parseGraphiteResponse(body)
}

func parseGraphiteResponse(body []byte) Datapoints {
  var dps Datapoints
  var res []target

  decoder := json.NewDecoder(bytes.NewBuffer(body))

  // Important to distinguish between ints and floats.
  decoder.UseNumber()

  dps.err = decoder.Decode(&res)
  if len(res) == 0 {
    dps.err = errors.New("Unexpected Graphite response. No targets were returned.")
  }
  if len(res) > 1 {
    dps.err = errors.New("Unexpected Graphite response. More than one target was returned.")
  }
  if dps.err != nil {
    return dps
  }

  // TODO: Check the query is the same as the target name.

  dps.points = res[0].Datapoints
  return dps
}

type queryResult []target

type target struct {
  Target string `json:"target"`

  // Datapoints are either
  //
  //     [[FLOAT, INT], ..., [FLOAT, INT]]  (type []intDatapoint).
  //
  // or
  //
  //     [[FLOAT, FLOAT], ..., [FLOAT, FLOAT]] (type []floatDatapoint).
  Datapoints [][]interface{} `json:"datapoints"`
}
