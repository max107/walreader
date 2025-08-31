package walreader

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	Namespace = "walreader"
)

func createGauge(name, help string) prometheus.Gauge {
	return promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      name,
		Help:      help,
	})
}

var (
	totalInsert           = createGauge("insert", "total number of insert operation message")
	totalUpdate           = createGauge("update", "total number of update operation message")
	totalDelete           = createGauge("delete", "total number of delete operation message")
	totalTruncate         = createGauge("truncate", "total number of truncate operation message")
	latency               = createGauge("receive_latency", "latest consumed message latency ms")
	processLatency        = createGauge("process_latency", "latest cdc process latency")
	slotActivity          = createGauge("slot_is_active", "whether the replication slot is active or not")
	slotConfirmedFlushLSN = createGauge("slot_confirmed_flush_lsn", "last lsn confirmed flushed to the replication slot")
	slotCurrentLSN        = createGauge("slot_current_lsn", "current lsn")
	slotRetainedWALSize   = createGauge("slot_retained_wal_size", "current lsn - restart lsn")
	slotLag               = createGauge("slot_lag", "current lsn - confirmed flush lsn")
)

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		totalInsert,
		totalUpdate,
		totalDelete,
		latency,
		processLatency,
		slotActivity,
		slotConfirmedFlushLSN,
		slotCurrentLSN,
		slotRetainedWALSize,
		slotLag,
	}
}
