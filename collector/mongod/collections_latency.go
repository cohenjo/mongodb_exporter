package mongod

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	// collectionSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	// 	Namespace: Namespace,
	// 	Subsystem: "db_coll",
	// 	Name:      "size",
	// 	Help:      "The total size in memory of all records in a collection",
	// }, []string{"db", "coll"})
	collectionLatenciesHistogram = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "db_coll_latencies_histogram",
		Help:      "collection latencies histogram statistics of mongod",
	}, []string{"db", "coll", "type", "micros"})
)

// CollectionStatList contains stats from all collections
type CollectionLatList struct {
	Members []CollectionLatency
}

// CollectionLatency contains latancy stats from a single collection
type CollectionLatency struct {
	Database     string
	Name         string
	Ns           string          `bson:"ns"`
	LocalTime    time.Time       `bson:"localTime"`
	LatencyStats OpLatenciesStat `bson:"latencyStats"`
}

// Update update each metric
func (ls *LatencyStat) UpdateWithLabels(op string, labels prometheus.Labels, gv *prometheus.GaugeVec) {
	if ls.Histogram != nil {
		for _, bucket := range ls.Histogram {
			labels["type"] = op
			labels["micros"] = strconv.FormatInt(bucket.Micros, 10)
			gv.With(labels).Set(bucket.Count)
		}
	}
	// opLatenciesTotal.WithLabelValues(op).Set(ls.Latency)
	// opLatenciesCountTotal.WithLabelValues(op).Set(ls.Ops)
}

// Export exports database stats to prometheus
func (collStatList *CollectionLatList) Export(ch chan<- prometheus.Metric) {
	log.Infoln("Starting export of latancy collections")
	// reset previously collected values
	collectionLatenciesHistogram.Reset()
	for _, member := range collStatList.Members {
		ls := prometheus.Labels{
			"db":   member.Database,
			"coll": member.Name,
		}
		stat := member.LatencyStats
		if stat.Reads != nil {
			stat.Reads.UpdateWithLabels("read", ls, collectionLatenciesHistogram)
		}
		if stat.Writes != nil {
			stat.Writes.UpdateWithLabels("write", ls, collectionLatenciesHistogram)
		}
		if stat.Commands != nil {
			stat.Commands.UpdateWithLabels("command", ls, collectionLatenciesHistogram)
		}

	}
	collectionLatenciesHistogram.Collect(ch)
}

// Describe describes database stats for prometheus
func (collStatList *CollectionLatList) Describe(ch chan<- *prometheus.Desc) {
	collectionLatenciesHistogram.Describe(ch)
}

// GetCollectionLatList returns latancies for a given database
func GetCollectionLatList(client *mongo.Client) *CollectionLatList {
	log.Infoln("Starting latancy collections")
	collectionLatList := &CollectionLatList{}
	dbNames, err := client.ListDatabaseNames(context.TODO(), bson.M{})
	if err != nil {
		_, logSFound := logSuppressCS[""]
		if !logSFound {
			log.Errorf("%s. Collection stats will not be collected. This log message will be suppressed from now.", err)
			logSuppressCS[""] = true
		}
		return nil
	}
	delete(logSuppressCS, "")
	for _, db := range dbNames {
		c, err := client.Database(db).ListCollections(context.TODO(), bson.M{}, options.ListCollections().SetNameOnly(true))
		if err != nil {
			_, logSFound := logSuppressCS[db]
			if !logSFound {
				log.Errorf("%s. Collection stats will not be collected for this db. This log message will be suppressed from now.", err)
				logSuppressCS[db] = true
			}
		} else {

			type collListItem struct {
				Name string `bson:"name,omitempty"`
				Type string `bson:"type,omitempty"`
			}

			delete(logSuppressCS, db)
			for c.Next(context.TODO()) {
				coll := &collListItem{}
				err := c.Decode(&coll)
				if err != nil {
					log.Error(err)
					continue
				}
				log.Infoln("latancy info for: ", coll.Name)
				collLatancy := CollectionLatency{}
				// err = client.Database(db).Collection(coll.Name).RunCommand(context.TODO(), bson.D{{"collStats", coll.Name}, {"scale", 1}}).Decode(&collStatus)
				pipline := []bson.M{{"$collStats": bson.M{"latencyStats": bson.M{"histograms": true}}}}

				cursor, err := client.Database(db).Collection(coll.Name).Aggregate(context.Background(), pipline)
				if err != nil {
					_, logSFound := logSuppressCS[db+"."+coll.Name]
					if !logSFound {
						log.Errorf("%s. Collection stats will not be collected for this collection. This log message will be suppressed from now.", err)
						logSuppressCS[db+"."+coll.Name] = true
					}
				} else {
					defer cursor.Close(context.Background())
					for cursor.Next(context.Background()) {
						err = cursor.Decode(&collLatancy)
						if err != nil {
							log.Errorf("%s. cursor.Decode Collection stats will not be collected for this collection. This log message will be suppressed from now.", err)
						}
						delete(logSuppressCS, db+"."+coll.Name)
						collLatancy.Database = db
						collLatancy.Name = coll.Name
						collectionLatList.Members = append(collectionLatList.Members, collLatancy)
					}
				}
			}
			if err := c.Close(context.TODO()); err != nil {
				log.Errorf("Could not close ListCollections() cursor, reason: %v", err)
			}
		}
	}
	log.Infoln("Finished latancy collections")
	return collectionLatList
}
