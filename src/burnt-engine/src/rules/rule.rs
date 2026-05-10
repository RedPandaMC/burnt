use crate::parse::import_map::ImportMap;
use crate::types::{Finding, Severity};

// ── Language filter ───────────────────────────────────────────────────────────

/// Which source language a rule applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanguageFilter {
    Python,
    Sql,
    Notebook,
    All,
}

impl LanguageFilter {
    /// Returns `true` if this filter matches the given language string.
    pub fn matches(self, language: &str) -> bool {
        let lang = language.to_lowercase();
        match self {
            LanguageFilter::All | LanguageFilter::Notebook => true,
            LanguageFilter::Python => lang == "python" || lang == "sdp",
            LanguageFilter::Sql => lang == "sql",
        }
    }

    /// Parse from a string (as stored in TOML rule files).
    pub fn for_language(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "python" | "sdp" => LanguageFilter::Python,
            "sql" => LanguageFilter::Sql,
            "notebook" => LanguageFilter::Notebook,
            _ => LanguageFilter::All,
        }
    }
}

// ── Rule metadata ─────────────────────────────────────────────────────────────

/// Static metadata for a rule — code, severity, human-readable strings.
pub struct RuleMeta {
    pub code: &'static str,
    pub severity: Severity,
    pub message: &'static str,
    pub suggestion: &'static str,
    pub category: &'static str,
}

// ── Analysis context ──────────────────────────────────────────────────────────

/// Rich context passed to every rule check.
///
/// The source is parsed once; rules read from this shared context rather than
/// re-parsing. `import_map` is always populated for Python/SDP sources; for SQL
/// sources it will contain an empty map.
pub struct AnalysisCtx<'src> {
    pub source: &'src str,
    pub language: &'src str,
    pub import_map: &'src ImportMap,
    /// The tree-sitter parse tree, `None` when the source failed to parse or
    /// when the language has no tree-sitter grammar configured.
    pub tree: Option<&'src tree_sitter::Tree>,
}

impl<'src> AnalysisCtx<'src> {
    pub fn new(
        source: &'src str,
        language: &'src str,
        import_map: &'src ImportMap,
        tree: Option<&'src tree_sitter::Tree>,
    ) -> Self {
        Self {
            source,
            language,
            import_map,
            tree,
        }
    }

    /// Returns `true` if the source originates from a DLT/SDP pipeline file
    /// (i.e. at least one pipeline namespace import is present).
    pub fn is_pipeline_context(&self) -> bool {
        self.import_map.pipeline_namespace().is_some()
    }
}

// ── Rule trait ────────────────────────────────────────────────────────────────

/// Every analysis rule implements this trait.
///
/// Rules are zero-sized unit structs — all state lives in `AnalysisCtx`.
///
/// ```rust
/// use crate::rules::rule::{Rule, RuleMeta, LanguageFilter, AnalysisCtx};
/// use crate::types::{Finding, Severity};
///
/// struct MyRule;
///
/// static MY_RULE_META: RuleMeta = RuleMeta {
///     code: "BP999",
///     severity: Severity::Warning,
///     message: "Example rule",
///     suggestion: "Fix it",
///     category: "BestPractice",
/// };
///
/// impl Rule for MyRule {
///     fn meta(&self) -> &'static RuleMeta { &MY_RULE_META }
///     fn language(&self) -> LanguageFilter { LanguageFilter::Python }
///     fn check(&self, ctx: &AnalysisCtx) -> Vec<Finding> { vec![] }
/// }
/// ```
pub trait Rule: Send + Sync {
    /// Static metadata (code, severity, message, suggestion, category).
    fn meta(&self) -> &'static RuleMeta;

    /// Which language(s) this rule applies to.
    fn language(&self) -> LanguageFilter;

    /// Run the rule against the provided context and return any findings.
    fn check(&self, ctx: &AnalysisCtx) -> Vec<Finding>;
}
