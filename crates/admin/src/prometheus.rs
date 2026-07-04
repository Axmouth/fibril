//! Prometheus text exposition rendering.
//!
//! Hand-rolled on purpose: everything Fibril exports is a counter or gauge
//! already held in atomics, so the format is a handful of `# HELP`,
//! `# TYPE`, and `name{labels} value` lines and a registry dependency would
//! buy nothing. Scrapes read snapshots, never hot paths.

use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
}

impl MetricKind {
    fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
        }
    }
}

/// One metric family: a name, help text, kind, and its samples. The HELP
/// and TYPE header renders exactly once per family regardless of sample
/// count, which is what the exposition format requires.
pub struct MetricFamily {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: MetricKind,
    pub samples: Vec<Sample>,
}

/// A single sample: label pairs (may be empty) and a value.
pub struct Sample {
    pub labels: Vec<(&'static str, String)>,
    pub value: f64,
}

impl Sample {
    pub fn plain(value: f64) -> Self {
        Self {
            labels: Vec::new(),
            value,
        }
    }

    pub fn labeled(labels: Vec<(&'static str, String)>, value: f64) -> Self {
        Self { labels, value }
    }
}

impl MetricFamily {
    /// A family with one unlabeled sample, the common node-level case.
    pub fn scalar(name: &'static str, help: &'static str, kind: MetricKind, value: f64) -> Self {
        Self {
            name,
            help,
            kind,
            samples: vec![Sample::plain(value)],
        }
    }
}

/// Render families into the text exposition format. Families with no
/// samples are skipped entirely (no orphan HELP/TYPE headers).
pub fn render(families: &[MetricFamily]) -> String {
    let mut out = String::new();
    for family in families {
        if family.samples.is_empty() {
            continue;
        }
        let _ = writeln!(out, "# HELP {} {}", family.name, escape_help(family.help));
        let _ = writeln!(out, "# TYPE {} {}", family.name, family.kind.as_str());
        for sample in &family.samples {
            if sample.labels.is_empty() {
                let _ = writeln!(out, "{} {}", family.name, format_value(sample.value));
            } else {
                let labels = sample
                    .labels
                    .iter()
                    .map(|(key, value)| format!("{key}=\"{}\"", escape_label_value(value)))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(
                    out,
                    "{}{{{labels}}} {}",
                    family.name,
                    format_value(sample.value)
                );
            }
        }
    }
    out
}

/// Integers render without a fractional part so counters read naturally.
fn format_value(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// Label values escape backslash, double quote, and newline per the format.
fn escape_label_value(raw: &str) -> String {
    raw.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Help text escapes backslash and newline (quotes are legal in help).
fn escape_help(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_help_type_once_and_samples_per_line() {
        let families = [MetricFamily {
            name: "fibril_test_total",
            help: "A test counter.",
            kind: MetricKind::Counter,
            samples: vec![
                Sample::labeled(vec![("topic", "orders".into())], 3.0),
                Sample::labeled(vec![("topic", "emails".into())], 7.0),
            ],
        }];
        let body = render(&families);
        assert_eq!(body.matches("# TYPE fibril_test_total").count(), 1);
        assert_eq!(body.matches("# HELP fibril_test_total").count(), 1);
        assert!(body.contains("fibril_test_total{topic=\"orders\"} 3\n"));
        assert!(body.contains("fibril_test_total{topic=\"emails\"} 7\n"));
        assert!(body.ends_with('\n'));
        assert!(!body.contains(" \n"), "no trailing whitespace: {body:?}");
    }

    #[test]
    fn escapes_label_values_and_help() {
        let families = [MetricFamily {
            name: "fibril_escape_total",
            help: "line one\nline \\two",
            kind: MetricKind::Counter,
            samples: vec![Sample::labeled(vec![("name", "a\"b\\c\nd".into())], 1.0)],
        }];
        let body = render(&families);
        assert!(body.contains(r#"# HELP fibril_escape_total line one\nline \\two"#));
        assert!(body.contains(r#"{name="a\"b\\c\nd"}"#), "{body}");
    }

    #[test]
    fn skips_empty_families_and_formats_values() {
        let families = [
            MetricFamily {
                name: "fibril_empty",
                help: "never rendered",
                kind: MetricKind::Gauge,
                samples: vec![],
            },
            MetricFamily::scalar("fibril_int", "int", MetricKind::Gauge, 42.0),
            MetricFamily::scalar("fibril_frac", "frac", MetricKind::Gauge, 0.5),
        ];
        let body = render(&families);
        assert!(!body.contains("fibril_empty"));
        assert!(body.contains("fibril_int 42\n"));
        assert!(body.contains("fibril_frac 0.5\n"));
    }
}
