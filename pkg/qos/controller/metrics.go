package controller

import "github.com/cockroachdb/cockroach/pkg/util/metric"

type Metrics struct {
	AdmissionLevel *metric.Gauge
	RejectionLevel *metric.Gauge
	CurNumBlocked  *metric.Gauge
	NumBlocked     *metric.Counter
	NumUnblocked   *metric.Counter
	NumTicks       *metric.Counter
	Inc            *metric.Counter
	Dec            *metric.Counter
}

func makeMeta(md metric.Metadata, name string) metric.Metadata {
	md.Name = name + "." + md.Name
	return md
}

func makeMetrics(name string) Metrics {
	return Metrics{
		AdmissionLevel: metric.NewGauge(makeMeta(metaAdmissionLevel, name)),
		RejectionLevel: metric.NewGauge(makeMeta(metaRejectionLevel, name)),
		CurNumBlocked:  metric.NewGauge(makeMeta(metaCurNumBlocked, name)),
		NumBlocked:     metric.NewCounter(makeMeta(metaNumBlocked, name)),
		NumUnblocked:   metric.NewCounter(makeMeta(metaNumUnblocked, name)),
		NumTicks:       metric.NewCounter(makeMeta(metaNumTicks, name)),
		Inc:            metric.NewCounter(makeMeta(metaNumInc, name)),
		Dec:            metric.NewCounter(makeMeta(metaNumDec, name)),
	}
}

var (
	metaAdmissionLevel = metric.Metadata{
		Name:        "admission.level",
		Help:        "Current admission level",
		Unit:        metric.Unit_COUNT,
		Measurement: "admission level",
	}
	metaRejectionLevel = metric.Metadata{
		Name:        "rejection.level",
		Help:        "Current rejection level",
		Unit:        metric.Unit_COUNT,
		Measurement: "rejection level",
	}
	metaNumTicks = metric.Metadata{
		Name:        "admission.ticks",
		Help:        "Number of ticks",
		Unit:        metric.Unit_COUNT,
		Measurement: "ticks",
	}
	metaNumDec = metric.Metadata{
		Name:        "admission.decrease",
		Help:        "Number of times the tick has decreased the level",
		Unit:        metric.Unit_COUNT,
		Measurement: "ticks",
	}
	metaNumInc = metric.Metadata{
		Name:        "admission.increase",
		Help:        "Number of times the tick has increase the level",
		Unit:        metric.Unit_COUNT,
		Measurement: "ticks",
	}
	metaCurNumBlocked = metric.Metadata{
		Name:        "admission.cur_num_blocked",
		Help:        "Gauge of currently blocked requests",
		Unit:        metric.Unit_COUNT,
		Measurement: "requests",
	}
	metaNumBlocked = metric.Metadata{
		Name:        "admission.num_blocked",
		Help:        "Counter of blocked requests",
		Unit:        metric.Unit_COUNT,
		Measurement: "requests",
	}
	metaNumUnblocked = metric.Metadata{
		Name:        "admission.num_unblocked",
		Help:        "Counter of unblocked requests",
		Unit:        metric.Unit_COUNT,
		Measurement: "requests",
	}
)
