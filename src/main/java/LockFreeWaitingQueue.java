import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
/**
 * Lock free waiting queue. <br>
 * After {@link LockFreeWaitingQueue#add(ReactorSemaphore.SubscribeTask)}, it
 * will start looping all tasks in queue until queue is empty. <br>
 * the sequence is start from acquire permit, poll queue and execute task, until
 * queue is empty. (permit will be released when task is terminate
 * automatically)<br>
 * When no more task in queue, then after releasing permit and check empty queue
 * again to guarantee at least one worker thread still is running if any task
 * still available in queue.
 *
 */
@Slf4j
@RequiredArgsConstructor
class LockFreeWaitingQueue {
	/**
	 * waiting queue which wait acquired permit then do subscribe.
	 */
	private final java.util.Queue<ReactorSemaphore.SubscribeTask> subscribeQueue = new ConcurrentLinkedQueue<>();
	/**
	 * permits
	 */
	@Getter
	private final Permits permits;

	/**
	 * tasks add counter. to judge empty queue as atomic operation.
	 */
	private final AtomicInteger taskCount = new AtomicInteger();

	/**
	 * try to acquire lock and run next mono subscribe <br>
	 */
	void trySuccessor() {
		boolean retry = true;
		//
		// retry until can't get permit or
		// get permit and after release permit but queue is empty.
		//
		int taskIndex = taskCount.get();
		while (retry) {
			boolean acquire = permits.tryAcquire();
			if (!acquire) {
				// can't get permit, return immediately.
				return;
			}
			ReactorSemaphore.SubscribeTask doSubscribeMono = subscribeQueue.poll();
			if (doSubscribeMono != null) {
				// do next subscribe
				log.debug("subscribe next");
				doSubscribeMono.subscribe();
			} else {
				// no next mono, release permit immediately.
				log.debug("no more task");
				permits.release();

				// after release permit, need to check queue again to prevent
				// | lock -> no task -> ............ release -> *need to check empty again and
				// retry here.
				// + ............ add task -> lock fail
				// if task count doesn't change and poll nothing, then it's still empty queue
				int tmp = taskCount.get();
				retry = taskIndex != tmp;
				taskIndex = tmp;
			}
		}
	}

	/**
	 * add into queue
	 *
	 * @param doSubscribe
	 *            do subscribe.
	 */
	void add(ReactorSemaphore.SubscribeTask doSubscribe) {
		// need to add queue, then increase count.
		// |empty queue -> release lock -> not empty -> try acquire permit
		// +............add queue -> increase cnt ...-> try acquire permit
		//
		// if increase cnt, then add queue.
		// |empty queue -> release lock -> not empty -> try acquire permit -> non queue
		// + ................. increase cnt -> (sleep..........................)-> add
		// queue -----> try acquire permit fail
		//
		this.subscribeQueue.add(doSubscribe);
		this.taskCount.incrementAndGet();
		this.trySuccessor();
	}

	/**
	 * remove task from queue
	 *
	 * @param subscribeTask
	 *            subscribe task
	 */
	void remove(ReactorSemaphore.SubscribeTask subscribeTask) {
		if(subscribeQueue.remove(subscribeTask)){
			// removed from queue and try next.
			subscribeTask.subscribe();
			log.debug("subscription cancelled, remove from queue");
		}
	}

	/**
	 * queue size
	 *
	 * @return size
	 */
	int size() {
		return subscribeQueue.size();
	}
}
