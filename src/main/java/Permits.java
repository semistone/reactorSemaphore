import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import reactor.util.context.ContextView;

/**
 * Permits
 * 
 */
@Slf4j
class Permits {
	/**
	 * number of permits allow subscribing mono.
	 */
	private final AtomicInteger availablePermits;

	/**
	 * get remain permits
	 *
	 * @return remain permits
	 */
	public int getAvailablePermits() {
		return availablePermits.intValue();
	}

	/**
	 * new permits
	 *
	 * @param availablePermits
	 *            number of permits
	 */
	public Permits(int availablePermits) {
		this.availablePermits = new AtomicInteger(availablePermits);
	}

	/**
	 * Acquires a permit from this semaphore, only if one is available at the time
	 * of invocation.
	 *
	 * @return {@link Permits.Permit} or empty if acquire failure.
	 */
	public Optional<Permits.Permit<ContextView>> tryAcquire() {
		if (doAcquire()) {
			return Optional.of(new Permits.PermitImpl<>() {
				@Override
				public void release() {
					if (once.compareAndSet(false, true)) {
						doRelease();
					}
				}
			});
		} else {
			return Optional.empty();
		}
	}

	/**
	 * try to acquire permit by atomic integer
	 *
	 * @return success acquire permit or not.
	 */
	private boolean doAcquire() {
		AtomicBoolean isAcquired = new AtomicBoolean();
		int remain = availablePermits.updateAndGet(p -> {
			if (p > 0) {
				isAcquired.set(true);
				return p - 1;
			} else {
				isAcquired.set(false);
				return p;
			}
		});
		if (isAcquired.get()) {
			log.debug("try acquire success remain {}", remain);
			return true;
		} else {
			log.debug("try acquire fail remain {}", remain);
			return false;
		}
	}

	/**
	 * release permit
	 */
	private void doRelease() {
		int remain = availablePermits.incrementAndGet();
		log.debug("release permit remain {}", remain);
	}

	/**
	 * Permit lock
	 *
	 * @param <C>
	 *            type of context
	 */
	public interface Permit<C> {
		/**
		 * release permit. it could be call multiple times, but only release one permit.
		 */
		void release();
	}

	/**
	 * simple permit implementation.
	 * 
	 * @param <C>
	 *            type of context
	 */
	abstract static class PermitImpl<C> implements Permit<C> {
		/**
		 * control release once only.
		 */
		protected final AtomicBoolean once = new AtomicBoolean();
	}
}
