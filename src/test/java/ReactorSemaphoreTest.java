
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * todo: test no subscribe
 */
@Slf4j
class ReactorSemaphoreTest {

	enum TestType {
		EMPTY, NORMAL, ERROR
	}
	private ReactorSemaphore target;
	@ParameterizedTest
	@MethodSource("acquireParams")
	void testAcquire(int permits, TestType type, int delay) {
		int totalLoop = 5;
		int subSize = 2;
		target = new ReactorSemaphore(permits);
		AtomicInteger concurrent = new AtomicInteger();
		AtomicInteger cnt = new AtomicInteger();
		AtomicInteger cnt2 = new AtomicInteger();
		AtomicInteger maxConcurrent = new AtomicInteger();
		for (int i = 0; i < totalLoop; i++) {
			CompletableFuture<Mono<String>> acquire;
			Mono<String> test = Mono.just("test" + i);
			if (delay > 0) {
				test = test.delayElement(Duration.ofMillis(delay));
			}
			Mono<String> finalTest = test;
			if (type == TestType.EMPTY) {
				// do acquire
				acquire = CompletableFuture.supplyAsync(() -> target.acquire(
						lock -> finalTest.doOnSubscribe(s -> concurrent.incrementAndGet()).doOnTerminate(() -> {
							// only one concurrent.
							maxConcurrent.set(Math.max(maxConcurrent.get(), concurrent.get()));
							cnt.incrementAndGet();
							concurrent.decrementAndGet();
						})).then(Mono.empty()));

			} else {
				// do acquire
				acquire = CompletableFuture.completedFuture(target.acquire(lock -> finalTest.doOnSubscribe(e -> {
					concurrent.incrementAndGet();
				}).map(e -> {
					// only one concurrent.
					maxConcurrent.set(Math.max(maxConcurrent.get(), concurrent.get()));
					cnt.incrementAndGet();
					concurrent.decrementAndGet();
					if (type == TestType.ERROR) {
						throw new RuntimeException("test");
					} else {
						return e;
					}
				}).subscribeOn(Schedulers.boundedElastic())));
			}

			// do subscribe
			for (int j = 0; j < subSize; j++) {
				CompletableFuture.runAsync(() -> acquire.join().doOnSuccess(e -> {
					cnt2.incrementAndGet();
				}).doOnError(e -> {
					cnt2.incrementAndGet();
				}).subscribe());
			}
		}

		// verify
		// all success
		await().untilAtomic(cnt, is(greaterThanOrEqualTo(totalLoop)));
		// all subscribe success
		await().untilAtomic(cnt2, is(totalLoop * subSize));
		// concurrent is same as permit.
		Assertions.assertThat(maxConcurrent.get()).isEqualTo(permits);
	}

	private static Stream<Arguments> acquireParams() {
		return Stream.of(Arguments.of(1, TestType.NORMAL, 0), Arguments.of(2, TestType.NORMAL, 10),
				Arguments.of(1, TestType.EMPTY, 0), Arguments.of(2, TestType.EMPTY, 10),
				Arguments.of(1, TestType.ERROR, 0), Arguments.of(2, TestType.ERROR, 10));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, 200})
	void testCancel(int delay) {

		// test1 still in running
		Mono<String> test = Mono.just("test1").delayElement(Duration.ofMillis(delay));
		AtomicBoolean isSubscribe = new AtomicBoolean();
		// test2 is in queue
		Mono<String> test2 = Mono.just("test2").doOnSubscribe(s -> isSubscribe.set(true))
				.delayElement(Duration.ofSeconds(10));
		ReactorSemaphore reactorSemaphore = new ReactorSemaphore(1);
		Mono<String> acquire1 = reactorSemaphore.acquire(lock -> test);
		Mono<String> acquire2 = reactorSemaphore.acquire(lock -> test2);
		Assertions.assertThat(reactorSemaphore.getQueueLength()).isZero();
		// subscribe test1 and test2
		Disposable subscribe = acquire1.subscribe();
		if (delay == 0) {
			subscribe.dispose();
		}
		Disposable subscribe2 = acquire2.subscribe();
		if (delay >= 200) {
			await().untilAsserted(() -> Assertions.assertThat(reactorSemaphore.getQueueLength()).isEqualTo(1));
		}
		// cancel second one
		subscribe2.dispose();
		log.debug("cancel subscribe2");
		// test 3 could still success to acquire
		Assertions.assertThat(reactorSemaphore.acquire(lock -> Mono.just("test3")).block()).isEqualTo("test3");
		// test2 never been subscribed
		if (delay >= 1000) {
			Assertions.assertThat(isSubscribe).isFalse();
		}
		await().untilAsserted(() -> Assertions.assertThat(reactorSemaphore.availablePermits()).isEqualTo(1));
		Assertions.assertThat(reactorSemaphore.getQueueLength()).isZero();

	}


	@Test
	void testRetry() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		AtomicInteger atomicInteger = new AtomicInteger();
		AtomicInteger result = new AtomicInteger();
		semaphore.acquire(lock -> Mono.fromSupplier(atomicInteger::incrementAndGet)).repeat(3)
				.doOnNext(result::addAndGet).blockLast();
		// 1+2+3+4 == 10
		Assertions.assertThat(result.get()).isEqualTo(10);
	}

	@Test
	void testError() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		Mono<String> mono = semaphore.acquire((lock) -> {
			throw new RuntimeException("test");
		});
		for (int i = 0; i < 3; i++) {
			mono.subscribeOn(Schedulers.boundedElastic()).subscribe();
		}
		Assertions.assertThatThrownBy(mono::block).isInstanceOf(RuntimeException.class);
	}

	@Test
	void testDifferentType() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		Mono<String> testStr = semaphore.acquire(lock -> Mono.just("test"));
		Mono<Integer> testInt = semaphore.acquire(lock -> Mono.just(5));
		Assertions.assertThat(testInt.block()).isEqualTo(5);
		Assertions.assertThat(testStr.block()).isEqualTo("test");
	}

	@Test
	void testRelease() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		semaphore.acquire(lock -> {
			lock.release();
			return Mono.just("test").delayElement(Duration.ofSeconds(100));
		}).subscribe();
		Assertions.assertThat(semaphore.acquire(lock -> Mono.just("test")).block()).isEqualTo("test");
	}

	@Test
	void testTryAcquireWithTimeout() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		Assertions.assertThat(semaphore.acquire().block()).isTrue();
		Assertions.assertThat(semaphore.tryAcquire(1, TimeUnit.SECONDS).block()).isFalse();
	}


	@Test
	void testTimeout() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		Assertions.assertThat(semaphore.acquire().block()).isTrue();
		// still in queue
		Mono<String> test = semaphore.acquire(releaes -> Mono.just("test")).timeout(Duration.ofMillis(5));
		Assertions.assertThatThrownBy(test::block).hasCauseExactlyInstanceOf(TimeoutException.class);
		// not in queue
		semaphore.release();
		Assertions.assertThat(semaphore.availablePermits()).isEqualTo(1);
		test = semaphore.acquire(releaes -> Mono.just("test").delayElement(Duration.ofSeconds(5)))
				.timeout(Duration.ofMillis(50));
		Assertions.assertThatThrownBy(test::block).hasCauseExactlyInstanceOf(TimeoutException.class);
		// permit still one
		Assertions.assertThat(semaphore.availablePermits()).isEqualTo(1);

	}

	@Test
	void testTryAcquire() {
		ReactorSemaphore semaphore = new ReactorSemaphore(1);
		Assertions.assertThat(semaphore.tryAcquire()).isTrue();
		Assertions.assertThat(semaphore.tryAcquire()).isFalse();
	}
}
