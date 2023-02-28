package metrics

import (
	"fmt"
	"net/url"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/metrics/influxdb"
	"github.com/influxdata/influxdb/client"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	influxDBClientTimeout = 10 * time.Second
	influxDBPingInterval  = 5 * time.Second
)

// influxDB starts InfluxDB reporters which will post the from the given metrics.Registry at each d interval.
func influxDB(r metrics.Registry, d time.Duration, url, database, username, password, namespace string) {
	// start geth InfluxDB reporter
	go influxdb.InfluxDB(r, d, url, database, username, password, namespace)

	// start custom InfluxDB reporter
	rep, err := newInfluxReporter(taggableRegistry, d, url, database, username, password, namespace, nil)
	if err != nil {
		logrus.WithField("url", url).WithError(err).Warn("Failed to new custom InfluxDB reporter")
	}

	go rep.run()
}

// influxReporter periodically reports metrics data to InfluxDB.
// Note for now only taggable counter supported, for other counters please use geth metrics package instead.
type influxReporter struct {
	reg      metrics.Registry
	interval time.Duration

	url       url.URL
	database  string
	username  string
	password  string
	namespace string
	tags      map[string]string

	client *client.Client

	cache map[string]int64
}

func newInfluxReporter(
	r metrics.Registry,
	d time.Duration,
	dburl, database, username, password, namespace string,
	tags map[string]string,
) (rep *influxReporter, err error) {
	u, err := url.Parse(dburl)
	if err != nil {
		return nil, errors.WithMessagef(err, "unable to parse InfluxDB url %v", dburl)
	}

	rep = &influxReporter{
		reg:       r,
		interval:  d,
		url:       *u,
		database:  database,
		username:  username,
		password:  password,
		namespace: namespace,
		tags:      tags,
		cache:     make(map[string]int64),
	}

	if err := rep.makeClient(); err != nil {
		return nil, errors.WithMessage(err, "unable to make InfluxDB client")
	}

	return
}

func (r *influxReporter) makeClient() (err error) {
	r.client, err = client.NewClient(client.Config{
		URL:      r.url,
		Username: r.username,
		Password: r.password,
		Timeout:  influxDBClientTimeout,
	})

	return
}

func (r *influxReporter) run() {
	sendTicker := time.NewTicker(r.interval)
	defer sendTicker.Stop()

	pingTicker := time.NewTicker(influxDBPingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-sendTicker.C:
			if err := r.send(); err != nil {
				logrus.WithError(err).Warn("Unable to report metrics to InfluxDB")
			}
		case <-pingTicker.C:
			if _, _, err := r.client.Ping(); err != nil {
				logrus.WithError(err).Warn(
					"Got error while sending a ping to InfluxDB, trying to recreate client",
				)

				if err := r.makeClient(); err != nil {
					logrus.WithError(err).Warn("Failed to recreate InfluxDB client")
				}
			}
		}
	}
}

func (r *influxReporter) send() (err error) {
	var pts []client.Point

	r.reg.Each(func(name string, i interface{}) {
		now := time.Now()
		namespace := r.namespace

		switch metric := i.(type) {
		case TaggableCounterSet:
			for _, tc := range metric.SnapshotT() {
				v := tc.Count()
				l := r.cache[name+tc.tags.Md5()]

				if v == l { // skip due to no change
					continue
				}

				t := make(map[string]string, len(tc.tags)+len(r.tags))
				tss := []map[string]string{r.tags, tc.tags}
				for _, ts := range tss {
					for k, v := range ts {
						t[k] = v
					}
				}

				pts = append(pts, client.Point{
					Measurement: fmt.Sprintf("%s%s.count", namespace, name),
					Tags:        t,
					Fields: map[string]interface{}{
						"value": v - l,
					},
					Time: now,
				})
				r.cache[name] = v
			}
		default:
			// TODO add more metrics type support?
			err = errors.New("metrics type not supported")
		}
	})

	if err != nil {
		return err
	}

	_, err = r.client.Write(client.BatchPoints{
		Points:   pts,
		Database: r.database,
	})
	return err
}
