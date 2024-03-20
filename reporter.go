package flow_storage_tracker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	scan "github.com/onflow/flow-batch-scan"
)

const ReporterNamespace = "storage_tracker"

type Reporter interface {
	scan.StatusReporter

	ReportTotalAccountsIncreased(accounts uint64)
	ReportStorageUsedChanged(storageUsed int64)
}

type reporter struct {
	*scan.DefaultStatusReporter
}

func NewReporter(
	logger zerolog.Logger,
) Reporter {
	return &reporter{
		DefaultStatusReporter: scan.NewStatusReporter(
			ReporterNamespace,
			logger),
	}
}

var (
	totalAccounts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ReporterNamespace,
		Name:      "accounts_total",
		Help:      "The number of all accounts.",
	})
	totalStorageUsed = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ReporterNamespace,
		Name:      "storage_used",
		Help:      "The total amount of storage used [bytes].",
	})
)

func (r *reporter) ReportTotalAccountsIncreased(accounts uint64) {
	totalAccounts.Add(float64(accounts))
}

func (r *reporter) ReportStorageUsedChanged(storageUsed int64) {
	totalStorageUsed.Add(float64(storageUsed))
}
