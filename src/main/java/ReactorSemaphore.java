
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

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
	/**
	 * Creates a {@code ReactorSemaphore} with the given number of permits.
	 *
	 * @param permits
	 *            the initial number of permits available.
	 */
	public ReactorSemaphore(int permits) {
		this.scheduler = Schedulers.boundedElastic();
		this.queue = new LockFreeWaitingQueue(new Permits(permits));
	}

	/**
	 * Acquires permit from this semaphore <br>
	 * 
	 * @param monoFun
	 *            source mono supplier
	 * @param <T>
	 *            type of mono
	 * @return synchronized mono
	 */
	public <T> Mono<T> acquire(Function<Permits.Permit<ContextView>, Mono<T>> monoFun) {
		// sink
		SinkPublisher<T> sinkPublisher = new SinkPublisher<>(monoFun, queue, scheduler);
		return Mono.from(sinkPublisher);
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
	 * sink publisher <br>
	 * one subscribe mapping to one {@link Sinks#one()} and pass subscriber to it.
	 *
	 * @param <T>
	 *            type of publisher
	 */
	@RequiredArgsConstructor
	private static class SinkPublisher<T> implements Publisher<T> {
		/**
		 * source publisher supplier
		 */
		private final Function<Permits.Permit<ContextView>, Mono<T>> sourcePublisherSupplier;

		/**
		 * waiting queue
		 */
		private final LockFreeWaitingQueue queue;

		private final Scheduler scheduler;
		@Override
		public void subscribe(Subscriber<? super T> subscriber) {
			CoreSubscriber<T> coreSubscriber = (CoreSubscriber<T>)subscriber;
			Context context = coreSubscriber.currentContext();
			// each subscriber will assign a Sinks.One as target publisher
			// and set into subscribe tasks
			Sinks.One<T> targetPublisher = Sinks.one();
			// cancel flag
			AtomicBoolean isSubscriptionCancelled = new AtomicBoolean();
			final AtomicReference<SubscribeTask<T>> doSubscribeRef = new AtomicReference<>();
			targetPublisher.asMono().doOnSubscribe(s -> {
				doSubscribeRef.set(new SubscribeTask<>(sourcePublisherSupplier, targetPublisher, context,
						isSubscriptionCancelled, () ->
					scheduler.schedule(queue::trySuccessor)
				));
				log.debug("add into queue");
				// add subscribe tasks into queue
				queue.add(doSubscribeRef.get());

			}).doOnCancel(() -> {
				isSubscriptionCancelled.set(true);
				SubscribeTask<T> doSubscribe = doSubscribeRef.get();
				if (doSubscribe == null) {
					log.debug("cancel before subscribe");
					queue.trySuccessor();
					return;
				}
				// remove subscribe tasks from queue
				queue.remove(doSubscribe);
			}).subscribe(coreSubscriber);
		}

	}

	/**
	 * subscribe task
	 * 
	 * @param <E>
	 *            type of publisher
	 */
	@RequiredArgsConstructor
	static class SubscribeTask<E> {
		/**
		 * source publisher
		 */
		private final Function<Permits.Permit<ContextView>, Mono<E>> sourcePublisher;

		/**
		 * target publisher
		 */
		private final Sinks.One<E> targetSink;

		/**
		 * reactor context
		 */
		private final ContextView context;

		/**
		 * is cancelled
		 */
		private final AtomicBoolean isCancelled;

		/**
		 * do next
		 */
		private final Runnable trySuccessor;

		/**
		 * subscription disposable
		 */
		private Disposable disposable;

		/**
		 * emit error handler
		 */
		private final MyEmitFailureHandler emitFailureHandler = new MyEmitFailureHandler();

		/**
		 * do subscribe
		 * 
		 * @param permitLock
		 *            permit lock, must call release after subscribe finished or
		 *            canceled.
		 * @return is subscribed
		 */
		public boolean subscribe(Permits.Permit<ContextView> permitLock) {
			((Permits.PermitImpl<ContextView>) permitLock).setContext(context);
			Mono<E> mono;
			try {
				mono = sourcePublisher.apply(permitLock);
			} catch (Exception e) {
				log.warn("get source publisher fail", e);
				targetSink.emitError(e, emitFailureHandler);
				permitLock.release();
				return false;
			}
			/*
			 * this mono has value or empty, if empty then need to emit empty event.
			 */
			AtomicBoolean hasValue = new AtomicBoolean();
			// subscribe mono and pass value/error to target sink publisher.
			disposable = mono.doOnCancel(() -> unparkSuccessor(permitLock))
					.doAfterTerminate(() -> unparkSuccessor(permitLock)).contextWrite(context).subscribe(e -> {
						hasValue.set(true);
						targetSink.emitValue(e, emitFailureHandler);
					}, ex -> targetSink.emitError(ex, emitFailureHandler), () -> {
						if (!hasValue.get()) {
							targetSink.emitEmpty(emitFailureHandler);
						}
					});
			// call dispose before set disposable into this.
			if (isCancelled.get() && !disposable.isDisposed()) {
				disposable.dispose();
			}
			return true;
		}

		/**
		 * dispose subscription
		 */
		public void dispose() {
			if (disposable != null && !disposable.isDisposed()) {
				log.debug("subscription cancelled, call dispose");
				disposable.dispose();
			}
		}

		/**
		 * release permit and try next
		 */
		private void unparkSuccessor(Permits.Permit<ContextView> releasePermit) {
			releasePermit.release();
			// try to run emit next queued sink.


			trySuccessor.run();
		}

	}

	/**
	 * simple error handler
	 */
	private static class MyEmitFailureHandler implements Sinks.EmitFailureHandler {
		@Override
		public boolean onEmitFailure(@NotNull SignalType signalType, Sinks.@NotNull EmitResult emitResult) {
			log.warn("emit fail signal type {}", signalType);
			return false;
		}
	}
}
