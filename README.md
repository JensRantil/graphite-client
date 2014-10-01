Graphite API
============
A simple client to query [the Graphite URL API](http://graphite.readthedocs.org/en/1.0/url-api.html).

No external dependencies are required (except Go standard library, that is).

How to install
--------------
Use `go get`:

    go get github.com/JensRantil/graphite-client

Documentation
-------------
Documentation can be found at
http://godoc.org/github.com/JensRantil/graphite-client.

A great document summarizing the Graphite API can be found [here](https://github.com/brutasse/graphite-api/blob/master/docs/api.rst).

Example
-------
```
import (
  "time"
  graphite "github.com/JensRantil/graphite-client"
)

func init() {
  client, err := graphite.New("http://mygraphite.com/render/")
  checkError(err)
  interval := TimeInterval{time.Now().Add(-10 * time.Minutes), time.Now()}
  values, err := client.QueryInts("myhost.category.value", interval.TimeInterval{time.})
  // ...

  // Equivalent to the query above.
  values, err := client.QueryIntsSince("myhost.category.value", 10 * time.Minutes)
  // ...
}
```

Contributing
------------
Feel free to fork at contribute pull requests. Please to add tests for new
feature and run them using `go test`.
