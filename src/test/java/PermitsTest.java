
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PermitsTest {

	@ParameterizedTest
	@ValueSource(ints = {1, 3})
	void testPermit(int permitCnt) {
		Permits permits = new Permits(permitCnt);
		AtomicInteger finish = new AtomicInteger();
		for (int i = 0; i < 100; i++) {
			CompletableFuture.runAsync(() -> {
				boolean retry = true;
				while (retry) {
					boolean acquire = permits.tryAcquire();
					if (acquire) {
						permits.release();
						finish.incrementAndGet();
						retry = false;
					}
				}
			});
		}
		await().untilAtomic(finish, is(100));
		Assertions.assertThat(permits.getAvailablePermits()).isEqualTo(permitCnt);
	}
}
