use crate::parse::namespace::NamespaceTracker;
use crate::types::Finding;

#[derive(Debug, Clone)]
pub struct ParsedSource<'a> {
    pub source: &'a str,
    pub tracker: NamespaceTracker,
}

impl<'a> ParsedSource<'a> {
    pub fn new(source: &'a str, tracker: NamespaceTracker) -> Self {
        Self { source, tracker }
    }
}

#[derive(Debug, Clone)]
pub struct RuleContext<'a> {
    pub source: &'a str,
    pub tracker: &'a NamespaceTracker,
}

impl<'a> RuleContext<'a> {
    pub fn new(source: &'a str, tracker: &'a NamespaceTracker) -> Self {
        Self { source, tracker }
    }

    pub fn is_sdp_context(&self) -> bool {
        self.tracker.dlt_namespace().is_some()
    }

    pub fn is_spark_call(&self, call_text: &str) -> bool {
        if let Some((ns, _)) = self.tracker.extract_call_parts(call_text) {
            return self.tracker.is_spark_namespace(ns);
        }
        false
    }

    pub fn is_dlt_call(&self, call_text: &str) -> bool {
        if let Some((ns, _)) = self.tracker.extract_call_parts(call_text) {
            return self.tracker.is_dlt_namespace(ns);
        }
        false
    }

    pub fn get_namespace_for_call<'b>(&self, call_text: &'b str) -> Option<&'b str> {
        self.tracker.resolve_call_namespace(call_text)
    }

    pub fn is_spark_method(&self, method: &str) -> bool {
        self.tracker.is_spark_method(method)
    }
}

#[derive(Debug, Clone)]
pub struct AnalysisContext {
    pub findings: Vec<Finding>,
    pub tracker: NamespaceTracker,
}

impl AnalysisContext {
    pub fn new(tracker: NamespaceTracker) -> Self {
        Self {
            findings: Vec::new(),
            tracker,
        }
    }

    pub fn add_findings(&mut self, new_findings: Vec<Finding>) {
        self.findings.extend(new_findings);
    }
}
