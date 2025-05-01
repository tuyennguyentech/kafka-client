use std::time::Duration;

#[derive(Debug)]
pub struct Metadata {
    /// How frequently to refresh the cluster metadata in the background.
    ///
    /// Default to 10 minutes. Set to `Duration::ZERO` to disable.
    pub refresh_frequency: Duration,
}

impl Metadata {
    pub fn with_refresh_frequency(mut self, refresh_frequency: Duration) -> Self {
        self.refresh_frequency = refresh_frequency;
        self
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata { refresh_frequency: Duration::from_secs(10 * 60) }
    }
}