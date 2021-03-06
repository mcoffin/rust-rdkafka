use client::{Context, EmptyContext};
use config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use producer::{BaseProducer, DeliveryResult, EmptyProducerContext, ProducerContext};
use statistics::Statistics;
use error::{KafkaError, KafkaResult, RDKafkaError};
use message::{Message, OwnedMessage, Timestamp, ToBytes};

use futures::{self, Canceled, Complete, Future, Poll, Oneshot, Async};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

//
// ********** POLLING PRODUCER **********
//

/// A producer with a separate thread for event handling.
///
/// The `PollingProducer` is a `BaseProducer` with a separate thread dedicated to calling `poll` at
/// regular intervals, in order to execute any queued event, such as delivery notifications. The
/// thread will be automatically stopped when the producer is dropped.
#[must_use = "The polling producer will stop immediately if unused"]
pub struct PollingProducer<C: ProducerContext + 'static> {
    producer: BaseProducer<C>,
    should_stop: Arc<AtomicBool>,
    handle: RwLock<Option<JoinHandle<()>>>,
}

impl FromClientConfig for PollingProducer<EmptyProducerContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<PollingProducer<EmptyProducerContext>> {
        PollingProducer::from_config_and_context(config, EmptyProducerContext)
    }
}

impl<C: ProducerContext + 'static> FromClientConfigAndContext<C> for PollingProducer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<PollingProducer<C>> {
        let polling_producer = PollingProducer {
            producer: BaseProducer::from_config_and_context(config, context)?,
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: RwLock::new(None),
        };
        polling_producer.start();
        Ok(polling_producer)
    }
}

impl<C: ProducerContext + 'static> PollingProducer<C> {
    /// Starts the polling thread that will drive the producer.
    fn start(&self) {
        let producer_clone = self.producer.clone();
        let should_stop = self.should_stop.clone();
        let handle = thread::Builder::new()
            .name("polling thread".to_string())
            .spawn(move || {
                trace!("Polling thread loop started");
                loop {
                    let n = producer_clone.poll(100);
                    if n == 0 {
                        if should_stop.load(Ordering::Relaxed) {
                            // We received nothing and the thread should
                            // stop, so break the loop.
                            break;
                        }
                    } else {
                        trace!("Received {} events", n);
                    }
                }
                trace!("Polling thread loop terminated");
            })
            .expect("Failed to start polling thread");
        let mut handle_store = self.handle.write().expect("poison error");
        *handle_store = Some(handle);
    }

    /// Stops the polling thread.
    fn stop(&self) {
        let mut handle_store = self.handle.write().expect("poison error");
        if (*handle_store).is_some() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match (*handle_store).take().unwrap().join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
    }

    /// Sends a message to Kafka. See the documentation in `BaseProducer`.
    fn send_copy<P, K>(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>,
        delivery_context: Option<Box<C::DeliveryContext>>,
    ) -> KafkaResult<()>
    where K: ToBytes + ?Sized,
          P: ToBytes + ?Sized {
        self.producer.send_copy(topic, partition, payload, key, delivery_context, timestamp)
    }

    /// Polls the internal producer. This is not normally required since the `PollingProducer` had
    /// a thread dedicated to calling `poll` regularly.
    fn poll(&self, timeout_ms: i32) {
        self.producer.poll(timeout_ms);
    }

    /// Flushes the producer. Should be called before termination.
    pub fn flush(&self, timeout_ms: i32) {
        self.producer.flush(timeout_ms);
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

impl<C: ProducerContext + 'static> Drop for PollingProducer<C> {
    fn drop(&mut self) {
        trace!("Destroy PollingProducer");
        self.stop();
    }
}


//
// ********** FUTURE PRODUCER **********
//

/// The `ProducerContext` used by the `FutureProducer`. This context will use a Future as its
/// `DeliveryContext` and will complete the future when the message is delivered (or failed to).
#[derive(Clone)]
struct FutureProducerContext<C: Context + 'static> {
    wrapped_context: C,
}

/// Represents the result of message production as performed from the `FutureProducer`.
///
/// If message delivery was successful, `OwnedDeliveryResult` will return the partition and offset
/// of the message. If the message failed to be delivered an error will be returned, together with
/// an owned copy of the original message.
type OwnedDeliveryResult = Result<(i32, i64), (KafkaError, OwnedMessage)>;

// Delegates all the methods calls to the wrapped context.
impl<C: Context + 'static> Context for FutureProducerContext<C> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.wrapped_context.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        self.wrapped_context.stats(statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.wrapped_context.error(error, reason);
    }
}

impl<C: Context + 'static> ProducerContext for FutureProducerContext<C> {
    type DeliveryContext = Complete<OwnedDeliveryResult>;

    fn delivery(&self, delivery_result: &DeliveryResult, tx: Complete<OwnedDeliveryResult>) {
        let owned_delivery_result = match delivery_result {
            &Ok(ref message) => Ok((message.partition(), message.offset())),
            &Err((ref error, ref message)) => Err((error.clone(), message.detach())),
        };
        let _ = tx.send(owned_delivery_result); // TODO: handle error
    }
}


/// A producer that returns a `Future` for every message being produced.
///
/// Since message production in rdkafka is asynchronous, the called cannot immediately know if the
/// delivery of the message was successful or not. The `FutureProducer` provides this information in
/// a `Future`, that will be completed once the information becomes available. This producer has an
/// internal polling thread and as such it doesn't need to be polled. It can be cheaply cloned to
/// get a reference to the same underlying producer. The internal thread can be terminated with the
/// `stop` method or moving the `FutureProducer` out of scope.
#[must_use = "Producer polling thread will stop immediately if unused"]
pub struct FutureProducer<C: Context + 'static> {
    producer: Arc<PollingProducer<FutureProducerContext<C>>>,
}

impl<C: Context + 'static> Clone for FutureProducer<C> {
    fn clone(&self) -> FutureProducer<C> {
        FutureProducer { producer: self.producer.clone() }
    }
}

impl FromClientConfig for FutureProducer<EmptyContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<FutureProducer<EmptyContext>> {
        FutureProducer::from_config_and_context(config, EmptyContext)
    }
}

impl<C: Context + 'static> FromClientConfigAndContext<C> for FutureProducer<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<FutureProducer<C>> {
        let future_context = FutureProducerContext { wrapped_context: context };
        let polling_producer = PollingProducer::from_config_and_context(config, future_context)?;
        Ok(FutureProducer { producer: Arc::new(polling_producer) })
    }
}

/// A `Future` wrapping the result of the message production.
///
/// Once completed, the future will contain an `OwnedDeliveryResult` with information on the
/// delivery status of the message.
pub struct DeliveryFuture {
    rx: Oneshot<OwnedDeliveryResult>,
}

// TODO: remove?
impl DeliveryFuture {
    pub fn close(&mut self) {
        self.rx.close();
    }
}

impl Future for DeliveryFuture {
    type Item = OwnedDeliveryResult;
    type Error = Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(owned_delivery_result)) => Ok(Async::Ready(owned_delivery_result)),
            Err(Canceled) => Err(Canceled),
        }
    }
}

impl<C: Context + 'static> FutureProducer<C> {
    /// Sends a copy of the payload and key provided to the specified topic. When no partition is
    /// specified the underlying Kafka library picks a partition based on the key, or a random one
    /// if the key is not specified. Returns a `DeliveryFuture` that will eventually contain the
    /// result of the send. The `block_ms` parameter will control for how long the producer
    /// is allowed to block if the queue is full. Set it to -1 to block forever, or 0 to never block.
    /// If `block_ms` is reached and the queue is still full, a `RDKafkaError::QueueFull` will be
    /// reported in the `DeliveryFuture`.
    pub fn send_copy<P, K>(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: Option<&P>,
        key: Option<&K>,
        timestamp: Option<i64>,
        block_ms: i64,
    ) -> DeliveryFuture
    where K: ToBytes + ?Sized,
          P: ToBytes + ?Sized {
        let start_time = Instant::now();

        loop {
            let (tx, rx) = futures::oneshot();
            match self.producer.send_copy(topic, partition, payload, key, timestamp, Some(Box::new(tx))) {
                Ok(_) => break DeliveryFuture{ rx },
                Err(e) => {
                    if let KafkaError::MessageProduction(RDKafkaError::QueueFull) = e {
                        if block_ms == -1 {
                            continue;
                        } else if block_ms > 0 && start_time.elapsed() < Duration::from_millis(block_ms as u64) {
                            self.poll(100);
                            continue;
                        }
                    }
                    let (tx, rx) = futures::oneshot();
                    let owned_message = OwnedMessage::new(
                        payload.map(|p| p.to_bytes().to_vec()),
                        key.map(|k| k.to_bytes().to_vec()),
                        topic.to_owned(),
                        timestamp.map_or(Timestamp::NotAvailable, |millis| Timestamp::CreateTime(millis)),
                        partition.unwrap_or(-1),
                        0
                    );
                    let _ = tx.send(Err((e, owned_message)));
                    break DeliveryFuture { rx };
                }
            }
        }
    }

    /// Stops the internal polling thread. The thread can also be stopped by moving
    /// the `FutureProducer` out of scope.
    pub fn stop(&self) {
        self.producer.stop();
    }

    /// Polls the internal producer. This is not normally required since the `PollingProducer` had
    /// a thread dedicated to calling `poll` regularly.
    pub fn poll(&self, timeout_ms: i32) {
        self.producer.poll(timeout_ms);
    }

    /// Flushes the producer. Should be called before termination.
    pub fn flush(&self, timeout_ms: i32) {
        self.producer.flush(timeout_ms);
    }

    /// Returns the number of messages waiting to be sent, or send but not acknowledged yet.
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

#[cfg(test)]
mod tests {
    // Just test that there are no panics, and that each struct implements the expected
    // traits (Clone, Send, Sync etc.). Behavior is tested in the integrations tests.
    use super::*;
    use config::ClientConfig;

    struct TestContext;

    impl Context for TestContext {}
    impl ProducerContext for TestContext {
        type DeliveryContext = i32;

        fn delivery(&self, _: &DeliveryResult, _: Self::DeliveryContext) {
            unimplemented!()
        }
    }

    // Verify that the future producer is clone, according to documentation.
    #[test]
    fn test_future_producer_clone() {
        let producer = ClientConfig::new().create::<FutureProducer<_>>().unwrap();
        let _producer_clone = producer.clone();
    }

    // Test that the future producer can be cloned even if the context is not Clone.
    #[test]
    fn test_base_future_topic_send_sync() {
        let test_context = TestContext;
        let producer = ClientConfig::new()
            .create_with_context::<TestContext, FutureProducer<_>>(test_context)
            .unwrap();
        let _producer_clone = producer.clone();
    }
}
