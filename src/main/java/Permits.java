import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

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


	public void release() {
		this.availablePermits.incrementAndGet();
	}

	/**
	 * try to acquire permit by atomic integer
	 *
	 * @return success acquire permit or not.
	 */
	public boolean tryAcquire() {
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
	 * Permit lock
	 *
	 */
	public interface Permit {
		/**
		 * release permit. it could be call multiple times, but only release one permit.
		 */
		void release();
	}

	/**
	 * simple permit implementation.
	 */
	abstract static class PermitImpl implements Permit {
		/**
		 * control release once only.
		 */
		protected final AtomicBoolean once = new AtomicBoolean();
	}
}
