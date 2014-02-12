import java.io.Serializable;

abstract class ClockService {

	public void ticks() {
	}
}

interface ClockFactory {

	ClockService getClock();
}

class LogicalClock extends ClockService {

	private int processNo;

	LogicalTimeStamps internalLogicalClock;

	private LogicalClock() {
		internalLogicalClock = new LogicalTimeStamps(this.processNo, 0);
	}

	public void setProcessNo(int processNo) {
		this.processNo = processNo;
		this.internalLogicalClock.processNo = processNo;
	}

	public static ClockFactory factory = new ClockFactory() {

		public ClockService getClock() {
			return new LogicalClock();
		}
	};

	public synchronized void ticks() {
		this.internalLogicalClock.timeStamp++;
	}
}

class VectorClock extends ClockService {

	private int processNo;

	private int processCount;

	VectorTimeStamps internalVectorClock;

	public void initializeTimeStamps(int processNo, int processCount) {
		this.processCount = processCount;
		this.processNo = processNo;
		this.internalVectorClock = new VectorTimeStamps(new int[this.processCount]);
		for (int i = 0; i < processCount; i++) {
			this.internalVectorClock.timeStampMatrix[i] = 0;
		}
	}

	private VectorClock() {
		internalVectorClock = new VectorTimeStamps(new int[this.processCount]);
	}

	public static ClockFactory factory = new ClockFactory() {

		public ClockService getClock() {
			return new VectorClock();
		}
	};

	public synchronized void ticks() {
		this.internalVectorClock.timeStampMatrix[this.processNo]++;
	}
}

enum ClockType {
	LOGICAL, VECTOR;
}

public class Clock {

	public static ClockService getClockService(ClockFactory factory) {
		return factory.getClock();
	}
}

class LogicalTimeStamps implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	int processNo;

	int timeStamp;

	public LogicalTimeStamps(int processNo, int timeStamp) {
		this.processNo = processNo;
		this.timeStamp = timeStamp;
	}
}

class VectorTimeStamps implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	int[] timeStampMatrix;

	public VectorTimeStamps(int[] timeStampMatrix) {
		this.timeStampMatrix = timeStampMatrix;
	}
}
