use std::time::Duration;

#[derive(Default, Debug)]
pub struct Producer { 
    // pub timeout: Duration,
    pub flush: Flush,
}


#[derive(Debug)]
pub struct Flush {
    /// The best-effort number of messages needed to trigger a flush.
    ///
    /// Default to 100.
    pub messages: i32,
    /// The best-effort frequency of flushes.
    ///
    /// Default to 5ms. Equivalent to [linger.ms](https://kafka.apache.org/documentation.html#producerconfigs_linger.ms).
    pub frequency: Duration,
}

impl Flush {
    pub fn with_messages(mut self, messages: i32) -> Self {
        self.messages = messages;
        self
    }
    pub fn with_frequency(mut self, frequency: Duration) -> Self {
        self.frequency = frequency;
        self
    }
}

impl Default for Flush {
    fn default() -> Self {
        Self { messages: 100, frequency: Duration::from_millis(5) }
    }
}

