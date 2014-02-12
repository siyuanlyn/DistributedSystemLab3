import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Comparator;

class LogicalLog implements Comparable<LogicalLog> {

	String concurrent = "    ";

	String processName;

	String event;

	Metadata metadata;

	int timestamp;

	public LogicalLog(TimeStampedMessage logical_log) {
		this.processName = logical_log.source;
		this.event = logical_log.kind.substring(3);
		this.metadata = new Metadata((Message) logical_log.data);
		this.timestamp = ((TimeStampedMessage) logical_log).getLogicalTimeStamps().timeStamp;
	}

	@Override
	public int compareTo(LogicalLog ll) {
		if (this.timestamp < ll.timestamp) {
			return -1;
		} else if (this.timestamp == ll.timestamp) {
			return 0;
		} else {
			return 1;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TimeStamp: " + timestamp + "; processName: " + processName + " ; Event: " + event);
		//sb.append(metadata.toString());
		return sb.toString();
	}
}

class LogicalLogComparator implements Comparator<LogicalLog> {

	public int compare(LogicalLog log1, LogicalLog log2) {
		if (log1.timestamp < log2.timestamp) {
			return -1;
		} else if (log1.timestamp == log2.timestamp) {
			return 0;
		} else {
			return 1;
		}
	}
}

class VectorLog implements Comparable<VectorLog> {

	String concurrent = "      ";

	String processName;

	String event;

	Metadata metadata;

	int[] timestamp;

	public VectorLog(TimeStampedMessage vector_log) {
		this.processName = vector_log.source;
		this.event = vector_log.kind.substring(3);
		if ((vector_log.data).getClass().equals(TimeStampedMessage.class)) {
			this.metadata = new Metadata((Message) vector_log.data);
		}
		this.timestamp = ((TimeStampedMessage) vector_log).getVectorTimeStamps().timeStampMatrix;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TimeStamp: " + Arrays.toString(timestamp) + "; processName: " + processName + " ; Event: " + event);
		return sb.toString();
	}

	@Override
	public int compareTo(VectorLog vl) {
		int i;
		for (i = 0; i < this.timestamp.length; i++) {
			if (this.timestamp[i] != vl.timestamp[i]) {
				break;
			}
		}
		if (i == this.timestamp.length) {
			return 0; // completely equal!
		} else {
			for (i = 0; i < this.timestamp.length; i++) {
				if (this.timestamp[i] <= vl.timestamp[i]) {
					continue;
				}
				break;
			}
			if (i == this.timestamp.length) {
				return -1; // not equal, happen before
			} else {
				for (i = 0; i < this.timestamp.length; i++) {
					if (this.timestamp[i] >= vl.timestamp[i]) {
						continue;
					}
					break;
				}
				if (i == this.timestamp.length) {
					return 1; // happen after
				} else {
					return 0; // concurrent
				}
			}
		}
	}
}

class VectorLogComparator implements Comparator<VectorLog> {

	public int compare(VectorLog log1, VectorLog log2) {
		int i;
		for (i = 0; i < log1.timestamp.length; i++) {
			if (log1.timestamp[i] != log2.timestamp[i]) {
				break;
			}
		}
		if (i == log1.timestamp.length) {
			return 0; // completely equal!
		} else {
			for (i = 0; i < log1.timestamp.length; i++) {
				if (log1.timestamp[i] <= log2.timestamp[i]) {
					continue;
				}
				break;
			}
			if (i == log1.timestamp.length) {
				return -1; // not equal, happen before
			} else {
				for (i = 0; i < log1.timestamp.length; i++) {
					if (log1.timestamp[i] >= log2.timestamp[i]) {
						continue;
					}
					break;
				}
				if (i == log1.timestamp.length) {
					return 1; // happen after
				} else {
					return 0; // concurrent
				}
			}
		}

	}
}

class Metadata {

	String msgSrc;

	String msgDst;

	String msgKind;

	int msgSeqNo;

	String msgAction;

	boolean msgDup;
	
	int groupNo;
	
	boolean isMulticast;

	int[] multicastVector;

	public Metadata(Message logMsg) {
		this.msgSrc = logMsg.source;
		this.msgDst = logMsg.destination;
		this.msgKind = logMsg.kind;
		this.msgSeqNo = logMsg.sequenceNumber;
		this.msgAction = logMsg.action;
		this.msgDup = logMsg.duplicate;
		this.groupNo = logMsg.groupNo;
		this.isMulticast = logMsg.multicast;
		this.multicastVector = logMsg.multicastVector;
	}

	public String toString() {
		return "[msgSrc=" + msgSrc + "; msgDst=" + msgDst + "; msgKind=" + msgKind + "; msgSeqNo=" + msgSeqNo + "; msgAction=" + msgAction + "; multicast=" + isMulticast + "; msgDup=" + msgDup + "]";
	}
}

public class CentralizedLogger {

	public static void main(String[] args) throws IOException {
		LoggerMessagePasser loggerMessagePasser = new LoggerMessagePasser(args[0], args[1]);
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter the clock type you want to set for nodes: logical or vector");
		String command = in.readLine();
		TimeStampedMessage setClockMessage;
		while (!(command.equalsIgnoreCase("logical") || command.equalsIgnoreCase("vector"))) {
			System.err.println("Please enter \"logical\" or \"vector\"");
			command = in.readLine();
		}
		switch (command.toLowerCase()) {
			case "logical":
				System.out.println("logical");
				loggerMessagePasser.clockType = ClockType.LOGICAL;
				for (String nodeName : loggerMessagePasser.streamMap.keySet()) {
					setClockMessage = new TimeStampedMessage(nodeName, "set_clock", null, ClockType.LOGICAL);
					setClockMessage.set_source(loggerMessagePasser.local_name);
					ObjectOutputStream oos = loggerMessagePasser.streamMap.get(nodeName);
					oos.writeObject(setClockMessage);
					oos.flush();
					oos.reset();
				}
				break;
			case "vector":
				System.out.println("vector");
				loggerMessagePasser.clockType = ClockType.VECTOR;
				for (String nodeName : loggerMessagePasser.streamMap.keySet()) {
					setClockMessage = new TimeStampedMessage(nodeName, "set_clock", null, ClockType.VECTOR);
					setClockMessage.set_source(loggerMessagePasser.local_name);
					ObjectOutputStream oos = loggerMessagePasser.streamMap.get(nodeName);
					oos.writeObject(setClockMessage);
					oos.flush();
					oos.reset();
				}
				break;
		}
		System.out.println("INFO: Logger time set done");
		while (true) {
			if (loggerMessagePasser.configurationFile.lastModified() > loggerMessagePasser.lastModifiedTime) {
				loggerMessagePasser.lastModifiedTime = loggerMessagePasser.configurationFile.lastModified();
				System.out.println("INFO: " + "configuration file modified!!!");
				loggerMessagePasser.nodeMap.clear();
				loggerMessagePasser.streamMap.clear();
				loggerMessagePasser.configList.clear();
				loggerMessagePasser.sendRuleList.clear();
				loggerMessagePasser.receiveRuleList.clear();
				loggerMessagePasser.serverSocket.close();
				loggerMessagePasser.parseConfigurationFile();
			}
			System.out.println("Enter the command you want to execute: retrieve");
			String cmd = in.readLine();
			switch (cmd.toLowerCase()) {
				case "retrieve":
					System.out.println(loggerMessagePasser.print());
					break;
				default:
					System.err.println("Illegal input format! Please enter again!");
			}
		}
	}
}
