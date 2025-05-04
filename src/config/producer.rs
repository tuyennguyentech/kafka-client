use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Producer {
    /// The level of acknowledgement reliability needed from the broker (defaults
    /// to All). Equivalent to the [`acks`](https://kafka.apache.org/documentation/#producerconfigs_acks) setting of the
    /// Producer Configs.
    pub required_acks: RequiredAcks,

    // pub compression: 

    /// The maximum duration the broker will wait the receipt of the number
    /// RequiredAcks (defaults to 10 seconds). This is only relevant when
    /// RequiredAcks is set to WaitForAll or a number > 1. Only supports
    /// millisecond resolution, nanoseconds will be truncated. Equivalent to
    /// the JVM producer's `request.timeout.ms` setting.
    pub timeout: Duration,
    /// The following config options control how often messages are batched up and
    /// sent to the broker. By default, messages are sent as fast as possible, and
    /// all messages received while the current batch is in-flight are placed
    /// into the subsequent batch.
    pub flush: Flush,
}

impl Producer {
    pub fn with_required_acks(mut self, required_acks: RequiredAcks) -> Self {
        self.required_acks = required_acks;
        self
    }

    pub fn with_timeout(mut self, mut timeout: Duration) -> Self {

        timeout = Duration::from_millis({
            let t = timeout.as_millis();
            if t >= i32::MAX as _ {
                panic!("producer.timeout is to big!");
            }
            t as _
        });
        self.timeout = timeout;
        self
    }

    pub fn with_flush(mut self, flush: Flush) -> Self {
        self.flush = flush;
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RequiredAcks {
    None = 0,
    One = 1,
    All = -1,
}

impl Default for Producer {
    fn default() -> Self {
        Self {
            required_acks: RequiredAcks::All,
            timeout: Duration::from_secs(10),
            flush: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
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
        Self {
            messages: 100,
            frequency: Duration::from_millis(2000),
        }
    }
}
