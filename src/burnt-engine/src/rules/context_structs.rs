use crate::parse::import_map::ImportMap;

#[derive(Debug, Clone)]
pub struct RuleContext<'a> {
    #[allow(dead_code)]
    pub source: &'a str,
    pub tracker: &'a ImportMap,
}

impl<'a> RuleContext<'a> {
    pub fn new(source: &'a str, tracker: &'a ImportMap) -> Self {
        Self { source, tracker }
    }

    pub fn is_sdp_context(&self) -> bool {
        self.tracker.pipeline_namespace().is_some()
    }
}
