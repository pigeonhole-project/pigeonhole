//! What a crate's metrics are: each crate lists the ones it records, the
//! binary describes them to the recorder and `docs/metrics.md` is made of them.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MetricDoc {
    pub name: &'static str,
    pub kind: MetricKind,
    pub labels: &'static [&'static str],
    pub help: &'static str,
}

impl MetricDoc {
    pub const fn counter(name: &'static str, labels: &'static [&'static str], help: &'static str) -> Self {
        Self { name, kind: MetricKind::Counter, labels, help }
    }

    pub const fn gauge(name: &'static str, labels: &'static [&'static str], help: &'static str) -> Self {
        Self { name, kind: MetricKind::Gauge, labels, help }
    }

    pub const fn histogram(name: &'static str, labels: &'static [&'static str], help: &'static str) -> Self {
        Self { name, kind: MetricKind::Histogram, labels, help }
    }
}
