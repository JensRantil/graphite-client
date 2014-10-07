package infrastructure

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	httpurl "net/url"
	"path"
	"time"
)

type Client struct {
	url    httpurl.URL
	Client *http.Client
}

// Create a new Client from a given URL. The URL is the base adress to
// Graphite, ie. without "/render" suffix etc.
func New(url string) (*Client, error) {
	u, err := httpurl.Parse(url)
	if err != nil {
		return nil, err
	}
	return &Client{
		url:    *u,
		Client: &http.Client{},
	}, nil
}

type MultiDatapoints []Datapoints

func (m MultiDatapoints) asMap() map[string]Datapoints {
	res := make(map[string]Datapoints)
	for _, point := range m {
		res[point.Target] = point
	}
	return res
}

// Create a new Client from a given URL. The URL is the base adress to
// Graphite, ie. without "/render" suffix etc.
func NewFromURL(url httpurl.URL) *Client {
	return &Client{url, &http.Client{}}
}

type TimeInterval struct {
	From time.Time
	To   time.Time
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
	Time  time.Time
	Value *float64
}

type IntDatapoint struct {
	Time  time.Time
	Value *int64
}

type Datapoints struct {
	// Previous error to make single queries nicer to work with.
	err    error
	Target string
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
				floatVal, err := jsonValue.Float64()
				if err != nil {
					return nil, errors.New("Value not proper number.")
				}
				*value = int64(floatVal)
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

func constructQueryPart(qs []string) httpurl.Values {
	query := make(httpurl.Values)
	for _, q := range qs {
		query.Add("target", q)
	}
	query.Add("format", "json")
	return query
}

type FindResultItem struct {
	Leaf          bool
	Text          string
	Id            string
	Expandable    bool
	AllowChildren bool
}

// Used to map isLeaf from integer to
type rawFindResultItem struct {
	Leaf          int    `json:"leaf"`
	Text          string `json:"text"`
	Id            string `json:"id"`
	Expandable    int    `json:"expandable"`
	AllowChildren int    `json:"allowChildren"`
}

type FindOpts struct {
	From  *time.Time
	Until *time.Time
}

func (g *Client) Find(query string, opts *FindOpts) ([]FindResultItem, error) {
	// Cloning to be able to modify.
	url := g.url
	url.Path = path.Join(url.Path, "/metrics/find")

	queryvalues := make(httpurl.Values)
	queryvalues.Add("query", query)
	if opts != nil && opts.From != nil {
		queryvalues.Add("from", graphiteDateFormat(*opts.From))
	}
	if opts != nil && opts.Until != nil {
		queryvalues.Add("until", graphiteDateFormat(*opts.Until))
	}
	url.RawQuery = queryvalues.Encode()

	resp, err := g.Client.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var res []rawFindResultItem
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&res)
	if err != nil {
		return nil, err
	}

	realResult := make([]FindResultItem, len(res))
	for i, item := range res {
		realResult[i].Id = item.Id
		realResult[i].Leaf = item.Leaf > 0
		realResult[i].Text = item.Text
		realResult[i].Expandable = item.Expandable > 0
		realResult[i].AllowChildren = item.AllowChildren > 0
	}

	return realResult, nil
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

// Fetches one or multiple Graphite series. Deferring identifying whether the
// result are ints of floats to later. Useful in clients that executes adhoc
// queries.
func (g *Client) QueryMulti(q []string, interval TimeInterval) (MultiDatapoints, error) {
	if err := interval.Check(); err != nil {
		return nil, err
	}

	// Cloning to be able to modify.
	url := g.url

	url.Path = path.Join(url.Path, "/render")

	queryPart := constructQueryPart(q)
	queryPart.Add("from", graphiteDateFormat(interval.From))
	queryPart.Add("until", graphiteDateFormat(interval.To))
	url.RawQuery = queryPart.Encode()

	resp, err := g.Client.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return parseGraphiteResponse(body)
}

// Fetches one or multiple Graphite series. Deferring identifying whether the
// result are ints of floats to later. Useful in clients that executes adhoc
// queries.
func (g *Client) QueryMultiSince(q []string, ago time.Duration) (MultiDatapoints, error) {
	if ago.Nanoseconds() <= 0 {
		return nil, errors.New("Duration is expected to be positive.")
	}

	// Cloning to be able to modify.
	url := g.url

	url.Path = path.Join(url.Path, "/render")

	queryPart := constructQueryPart(q)
	queryPart.Add("from", fmt.Sprintf("%dminutes", ago.Minutes()))
	url.RawQuery = queryPart.Encode()

	resp, err := g.Client.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return parseGraphiteResponse(body)
}

// Fetches a Graphite result only expecting one timeseries. Deferring
// identifying whether the result are ints of floats to later. Useful in
// clients that executes adhoc queries.
func (g *Client) Query(q string, interval TimeInterval) Datapoints {
	if err := interval.Check(); err != nil {
		return Datapoints{err, "", nil}
	}

	// Cloning to be able to modify.
	url := g.url

	url.Path = path.Join(url.Path, "/render")

	queryPart := constructQueryPart([]string{q})
	queryPart.Add("from", graphiteDateFormat(interval.From))
	queryPart.Add("until", graphiteDateFormat(interval.To))
	url.RawQuery = queryPart.Encode()

	resp, err := g.Client.Get(url.String())
	if err != nil {
		return Datapoints{err, "", nil}
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Datapoints{err, "", nil}
	}

	points, err := parseGraphiteResponse(body)
	return parseSingleGraphiteResponse(points, err)
}

func graphiteSinceString(duration time.Duration) string {
	return fmt.Sprintf("%dminutes", -int(duration.Minutes()+0.5))
}

func (g *Client) QuerySince(q string, ago time.Duration) Datapoints {
	if ago.Nanoseconds() <= 0 {
		return Datapoints{errors.New("Duration is expected to be positive."), "", nil}
	}

	// Cloning to be able to modify.
	url := g.url

	url.Path = path.Join(url.Path, "/render")

	queryPart := constructQueryPart([]string{q})
	queryPart.Add("from", graphiteSinceString(ago))
	url.RawQuery = queryPart.Encode()

	resp, err := http.Get(url.String())
	if err != nil {
		return Datapoints{err, "", nil}
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Datapoints{err, "", nil}
	}

	points, err := parseGraphiteResponse(body)
	return parseSingleGraphiteResponse(points, err)
}

func parseSingleGraphiteResponse(dpss []Datapoints, err error) (dps Datapoints) {
	if len(dpss) == 0 {
		dps.err = errors.New("Unexpected Graphite response. No targets were matched.")
	}
	if len(dpss) > 1 {
		dps.err = errors.New("Unexpected Graphite response. More than one target were returned.")
	}
	if dps.err != nil {
		return
	}

	dps.points = dpss[0].points

	return
}

func parseGraphiteResponse(body []byte) (MultiDatapoints, error) {
	var res []target

	decoder := json.NewDecoder(bytes.NewBuffer(body))

	// Important to distinguish between ints and floats.
	decoder.UseNumber()

	err := decoder.Decode(&res)
	datapoints := make([]Datapoints, len(res))
	for i, points := range res {
		datapoints[i].Target = points.Target
		datapoints[i].points = points.Datapoints
	}

	return MultiDatapoints(datapoints), err
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
