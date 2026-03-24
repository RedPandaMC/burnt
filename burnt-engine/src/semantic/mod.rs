pub mod scope;
pub mod bindings;

pub use self::scope::analyze_scope;
pub use self::bindings::analyze_bindings;