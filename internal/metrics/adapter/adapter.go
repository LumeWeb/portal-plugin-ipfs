package adapter

import (
	"github.com/ipfs/go-metrics-interface"
	"github.com/prometheus/client_golang/prometheus"
)

// InjectPrometheusAdapter injects a Prometheus implementation into go-metrics-interface.
// This allows boxo components (like bitswap) to emit metrics to the provided Prometheus registry.
// Call this once during plugin initialization before creating any IPFS components.
func InjectPrometheusAdapter(reg prometheus.Registerer) error {
	return metrics.InjectImpl(func(name, helptext string) metrics.Creator {
		return &prometheusCreator{
			reg:      reg,
			name:     name,
			helptext: helptext,
		}
	})
}

type prometheusCreator struct {
	reg      prometheus.Registerer
	name     string
	helptext string
}

func (p *prometheusCreator) Counter() metrics.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: p.name,
		Help: p.helptext,
	})
	p.reg.MustRegister(c)
	return &prometheusCounter{c: c}
}

func (p *prometheusCreator) CounterVec(labelNames []string) metrics.CounterVec {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.name,
		Help: p.helptext,
	}, labelNames)
	p.reg.MustRegister(cv)
	return &prometheusCounterVec{cv: cv}
}

func (p *prometheusCreator) Gauge() metrics.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: p.name,
		Help: p.helptext,
	})
	p.reg.MustRegister(g)
	return &prometheusGauge{g: g}
}

func (p *prometheusCreator) Histogram(buckets []float64) metrics.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    p.name,
		Help:    p.helptext,
		Buckets: buckets,
	})
	p.reg.MustRegister(h)
	return &prometheusHistogram{h: h}
}

func (p *prometheusCreator) Summary(opts metrics.SummaryOpts) metrics.Summary {
	so := prometheus.SummaryOpts{
		Name:       p.name,
		Help:       p.helptext,
		Objectives: opts.Objectives,
		MaxAge:     opts.MaxAge,
		AgeBuckets: opts.AgeBuckets,
		BufCap:     opts.BufCap,
	}
	s := prometheus.NewSummary(so)
	p.reg.MustRegister(s)
	return &prometheusSummary{s: s}
}

type prometheusCounter struct {
	c prometheus.Counter
}

func (p *prometheusCounter) Inc() {
	p.c.Inc()
}

func (p *prometheusCounter) Add(delta float64) {
	p.c.Add(delta)
}

type prometheusCounterVec struct {
	cv *prometheus.CounterVec
}

func (p *prometheusCounterVec) WithLabelValues(lvs ...string) metrics.Counter {
	return &prometheusCounter{c: p.cv.WithLabelValues(lvs...)}
}

type prometheusGauge struct {
	g prometheus.Gauge
}

func (p *prometheusGauge) Set(val float64) {
	p.g.Set(val)
}

func (p *prometheusGauge) Inc() {
	p.g.Inc()
}

func (p *prometheusGauge) Dec() {
	p.g.Dec()
}

func (p *prometheusGauge) Add(delta float64) {
	p.g.Add(delta)
}

func (p *prometheusGauge) Sub(delta float64) {
	p.g.Sub(delta)
}

type prometheusHistogram struct {
	h prometheus.Histogram
}

func (p *prometheusHistogram) Observe(val float64) {
	p.h.Observe(val)
}

type prometheusSummary struct {
	s prometheus.Summary
}

func (p *prometheusSummary) Observe(val float64) {
	p.s.Observe(val)
}
