
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Reactor Semaphore Acquire permit when mono been subscribed and release permit
 * after mono have been cancel or terminated. <br>
 *
 * <pre>
 *  {@code
 *     ReactorSemaphore reactorSemaphore = new ReactorSemaphore(1);
 *     Mono<?> cachedMono1 = reactorSemaphore.acquire(lock -> Mono.just("test1")...)
 *     Mono<?> cachedMono2 = reactorSemaphore.acquire(lock -> Mono.just("test2")...)
 *  }
 * </pre>
 * 
 * second mono will be subscribed after first one finish. <br>
 *
 * <h3>Implementation Details:</h3> <br>
 * When user calls {@link ReactorSemaphore#acquire(Function)}, it will return a
 * Mono immediately. everytime when user try to subscribe from
 * {@link Mono#subscribe(Subscriber)}, it will create a {@link Sinks.One} and
 * put into {@link SubscribeTask} and put this task into
 * {@link LockFreeWaitingQueue}. <br>
 * Inside this waiting queue it will loop all subscribe tasks one by one until
 * queue are empty or {@link Permits#tryAcquire()} failed.<br>
 *
 */
@Slf4j
public class ReactorSemaphore {

	/**
	 * waiting queue
	 */
	private final LockFreeWaitingQueue queue;

	private final Scheduler scheduler;
	private final Permits permits;
	/**
	 * Creates a {@code ReactorSemaphore} with the given number of permits.
	 *
	 * @param permits
	 *            the initial number of permits available.
	 */
	public ReactorSemaphore(int permits) {
		this.scheduler = Schedulers.boundedElastic();
		this.permits =new Permits(permits);
		this.queue = new LockFreeWaitingQueue(this.permits);
	}

	public <T> Mono<T> acquire(Function<Permits.Permit, Mono<T>> sourcePublisherSupplier) {
		return acquire().flatMap(b -> {
			Permits.PermitImpl permit = new Permits.PermitImpl() {
				@Override
				public void release() {
					if (once.compareAndSet(false, true)) {
						permits.release();
					}
				}
			};
			if (Boolean.FALSE.equals(b)) {
				Mono<T> empty = Mono.empty();
				return unparkSuccessor(permit, empty);
			}
			try {
				return unparkSuccessor(permit, sourcePublisherSupplier.apply(permit));
			} catch (Exception e) {
				return unparkSuccessor(permit, Mono.error(e));
			}
		});
	}

	private <T> Mono<T> unparkSuccessor(Permits.Permit releasePermit, Mono<T> mono) {
		return mono.doOnCancel(() -> unparkSuccessor(releasePermit))
				.doAfterTerminate(() -> unparkSuccessor(releasePermit));
	}

	/**
	 * release permit and try next
	 */
	private void unparkSuccessor(Permits.Permit releasePermit) {
		releasePermit.release();
		// try to run emit next queued sink.
		this.scheduler.schedule(this.queue::trySuccessor);
	}
	/**
	 * Acquires permit from this semaphore <br>
	 * 
	 * @return synchronized mono
	 */
	public Mono<Boolean> acquire() {
		return Mono.create(targetPublisher -> {
			// each subscriber will assign a MonoSink as target publisher
			// and set into subscribe tasks
			// cancel flag
			AtomicBoolean isSubscriptionCancelled = new AtomicBoolean();

			log.debug("add into queue");
			// add subscribe tasks into queue
			SubscribeTask task = new SubscribeTask(targetPublisher,
					isSubscriptionCancelled);
			queue.add(task);
			targetPublisher.onCancel(() -> {
				isSubscriptionCancelled.set(true);
				// remove subscribe tasks from queue
				queue.remove(task);
			});
		});

	}

	public Mono<Boolean> tryAcquire(long timeout, TimeUnit unit) {
		return acquire().timeout(Duration.ofNanos(unit.toNanos(timeout))).onErrorReturn(false);
	}

	public void release() {
		permits.release();
	}

	/**
	 * Returns an estimate of the number of monos waiting to acquire
	 * 
	 * @return the estimated number of monos waiting for this lock
	 */
	public int getQueueLength() {
		return this.queue.size();
	}

	/**
	 * Returns the current number of permits available in this semaphore.
	 * 
	 * @return the number of permits available in this semaphore
	 */
	public int availablePermits() {
		return this.queue.getPermits().getAvailablePermits();
	}


	/**
	 * subscribe task
	 *
	 */
	@RequiredArgsConstructor
	static class SubscribeTask {
		/**
		 * target publisher
		 */
		private final MonoSink<Boolean> targetSink;

		/**
		 * is cancelled
		 */
		private final AtomicBoolean isCancelled;


		/**
		 * do subscribe
		 *
		 */
		public void subscribe() {
			// subscribe mono and pass value/error to target sink publisher.
			targetSink.success(!isCancelled.get());
		}
	}

}
