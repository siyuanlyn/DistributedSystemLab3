import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;

class LoggerMessagePasser extends MessagePasser {

	ArrayList<LogicalLog> logicalLogList = new ArrayList<LogicalLog>();

	ArrayList<VectorLog> vectorLogList = new ArrayList<VectorLog>();

	LogicalLogComparator logicalLogComparator = new LogicalLogComparator();

	VectorLogComparator vectorLogComparator = new VectorLogComparator();

	public LoggerMessagePasser(String configuration_filename, String local_name)
			throws IOException {
		super(configuration_filename, local_name);
	}

	@Override
	public void startListenerThread() throws IOException {
		Thread loggerListenerThread = new LoggerListenerThread(this);
		loggerListenerThread.start();
	}

	public String print() {
		StringBuilder sb = new StringBuilder();
		if (this.clockType == ClockType.LOGICAL) {
			ArrayList<ArrayList<LogicalLog>> logs = new ArrayList<ArrayList<LogicalLog>>(this.processCount);
			for (int i = 0; i < this.processCount; i++) {
				ArrayList<LogicalLog> list = new ArrayList<LogicalLog>();
				logs.add(list);
			}
			synchronized (logicalLogList) {
				for (LogicalLog ll : logicalLogList) {
					int index = ProcessNo.valueOf(ll.processName.toUpperCase()).ordinal();

					ArrayList<LogicalLog> logList = logs.get(index);
					logList.add(ll);
				}
				for (ArrayList<LogicalLog> list : logs) {

					if (list.size() > 0) {
						Collections.sort(list);
						StringBuilder splitsb = new StringBuilder();
						for (int j = 0; j < list.get(0).metadata.toString().length(); j++)
							splitsb.append("=");
						splitsb.append("\n");

						String split = splitsb.toString();
						sb.append(split);
						String nodeTitle = "Node " + list.get(0).processName + "\n";
						for (int i = 0; i <= (split.length() - nodeTitle.length()) / 2; i++)
							sb.append(" ");
						sb.append(nodeTitle);
						for (int i = 0; i < list.size(); i++) {
							sb.append(list.get(i).toString() + " ");
							LogicalLog ll = list.get(i);

							if(ll.event.equalsIgnoreCase(Function.SEND.toString())){
								int index = ProcessNo.valueOf(ll.metadata.msgDst.toUpperCase()).ordinal();
								System.out.println("INFO: dest index" + index);
								ArrayList<LogicalLog> destList = logs.get(index);
								for(LogicalLog destLog : destList){
									if(destLog.event.equalsIgnoreCase(Function.RECEIVE.toString()) && destLog.metadata.msgSrc.equalsIgnoreCase(ll.metadata.msgSrc)
											&& destLog.metadata.msgSeqNo == ll.metadata.msgSeqNo){
										System.out.println("INFO: find receive");
										sb.append("\u2192");
										sb.append(" " + destLog.processName+" ("+destLog.timestamp+")");
										break;
									}

								}
							}
							sb.append("\n"+ll.metadata.toString()+"\n");
							// print arrow
							if (i != list.size() - 1) {
								for (int j = 0; j <= split.length() / 2; j++)
									sb.append(" ");
								sb.append("\u2193\n");
							}
						}
						sb.append(split);
					}
				}

			}
			return sb.toString();
		} else {
			int padSize = 0;
			HashMap<Integer, ArrayList<Integer>> hbs = new HashMap<Integer, ArrayList<Integer>>();
			HashMap<Integer, ArrayList<Integer>> cons = new HashMap<Integer, ArrayList<Integer>>();
			synchronized (vectorLogList) {
				for (int i = 0; i < vectorLogList.size(); i++) {
					VectorLog vl = vectorLogList.get(i);
					sb.append("Log#");
					sb.append(i + 1);
					sb.append(" : " + vl.toString());
					if (i == 0)
						padSize = vl.metadata.toString().length();
					int pad = padSize - vl.toString().length() - 8;
					while (pad > 0) {
						sb.append(" ");
						pad--;
					}
					sb.append("HB: ");
					ArrayList<Integer> hb = new ArrayList<Integer>();
					ArrayList<Integer> con = new ArrayList<Integer>();

					for (int j = 0; j < vectorLogList.size(); j++) {
						if (j != i) {
							int status = vl.compareTo(vectorLogList.get(j));
							if (status < 0)
								hb.add(j + 1);
							else if (status == 0)
								con.add(j + 1);
						}
					}
					if (hb.size() > 0)
						hbs.put(i + 1, hb);
					if (con.size() > 0)
						cons.put(i + 1, con);
					for (int num : hb) {
						sb.append(num + " ");
					}
					sb.append(" CON: ");
					for (int num : con) {
						sb.append(num + " ");
					}
					sb.append("\n" + vl.metadata.toString() + "\n");

				}

			}
			HashMap<Integer, ArrayList<ArrayList<Integer>>> hbLines = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();

			for (int logNum : hbs.keySet()) {
				ArrayList<Integer> hb = hbs.get(logNum);
				ArrayList<ArrayList<Integer>> hbline = new ArrayList<ArrayList<Integer>>();
				ArrayList<Integer> result = new ArrayList<Integer>();
				result.add(logNum);
				hbline.add(result);
				for (int i = 0; i < hb.size(); i++) {
					int node = hb.get(i);
					for (int k = 0; k < hbline.size(); k++) {
						ArrayList<Integer> line = hbline.get(k);
						if (isExisted(line, node))
							continue;
						int index = findIndex(line, node);

						if (index >= 0) {
							line.add(index, node);
							hbline.set(k, line);
						} else {
							index = Math.abs(index);
							boolean flag = false;
							for (int n : hb) {
								if (isBetween(line.get(index - 1), node, n)) {
									flag = true;
									break;
								}
							}
							if (!flag) {
								ArrayList<Integer> newLine = new ArrayList<Integer>();
								for (int j = 0; j < index; j++) {
									newLine.add(line.get(j));
								}
								newLine.add(node);
								hbline.add(newLine);
							}
						}
					}
				}
				if (hbline.size() > 0)
					hbLines.put(logNum, hbline);
			}
			sb.append("Happen before lines: \n");
			for (int start : hbLines.keySet()) {
				ArrayList<ArrayList<Integer>> hbLine = hbLines.get(start);
				for (ArrayList<Integer> line : hbLine) {
					for (int i = 0; i < line.size(); i++) {
						sb.append(line.get(i));
						if (i != line.size() - 1) {
							sb.append("\u2192");
						}
					}
					sb.append("\n");
				}
			}
			return sb.toString();
		}
	}

	private boolean isExisted(ArrayList<Integer> result, int log) {
		for (int a : result) {
			if (a == log)
				return true;
		}
		return false;
	}

	private boolean isBetween(int start, int end, int log) {
		VectorLog startvl = vectorLogList.get(start - 1);
		VectorLog endvl = vectorLogList.get(end - 1);
		VectorLog logvl = vectorLogList.get(log - 1);
		if (startvl.compareTo(logvl) == -1 && logvl.compareTo(endvl) == -1)
			return true;
		return false;

	}

	private int findIndex(ArrayList<Integer> result, int log) {
		VectorLog newvl = vectorLogList.get(log - 1);

		for (int i = 0; i < result.size(); i++) {
			VectorLog old = vectorLogList.get(result.get(i) - 1);
			// if new happen after old, should at least insert after this, for loop do it for us

			// new happen before old, since this list should be maintained as sorted, this old
			// index is the place to insert
			if (newvl.compareTo(old) < 0) {
				return i;
			} else if (newvl.compareTo(old) == 0) {
				// if new is concurrent with old, then we need to start a new list, return -1 * i to
				// indicate that new should start a new line with all nodes before i.
				// since the result always has the the head of the line, so i would always > 0.
				return -1 * i;
			}
		}
		// if we go through the whole list, it means we need to add it to the end of the list
		return result.size();

	}
}

public class MessagePasser {

	@SuppressWarnings("rawtypes")
	LinkedHashMap networkTable;

	HashMap<String, Node> nodeMap = new HashMap<String, Node>();

	HashMap<String, ObjectOutputStream> streamMap = new HashMap<String, ObjectOutputStream>();

	ServerSocket serverSocket;

	ConcurrentLinkedQueue<Message> messageQueue = new ConcurrentLinkedQueue<Message>();

	ConcurrentLinkedQueue<Message> delaySendingQueue = new ConcurrentLinkedQueue<Message>();

	ConcurrentLinkedQueue<Message> popReceivingQueue = new ConcurrentLinkedQueue<Message>();

	ConcurrentLinkedQueue<Message> delayReceivingQueue = new ConcurrentLinkedQueue<Message>();

	ConcurrentLinkedQueue<TimeStampedMessage> logQueue = new ConcurrentLinkedQueue<TimeStampedMessage>();
	
//	LinkedList<Message> holdBackList = new LinkedList<>(); 

	ArrayList<LinkedHashMap<String, String>> configList;

	ArrayList<LinkedHashMap<String, String>> sendRuleList;

	ArrayList<LinkedHashMap<String, String>> receiveRuleList;

	ArrayList<LinkedHashMap<String, String>> multicastGroupList;

	File configurationFile;

	long lastModifiedTime;

	String configuration_filename;

	String local_name;

	ClockService clockService = null;

	ClockType clockType = null;

	ProcessNo processNo = null;

	int processCount = 0;

	Function function = null;

	boolean log = false;

	Multicast multicast = new Multicast(this); 

	public void setClockService(ClockType clockType) {
		switch (clockType) {
		case LOGICAL:
			clockService = Clock.getClockService(LogicalClock.factory);
			((LogicalClock) clockService).setProcessNo(processNo.value);
			break;
		case VECTOR:
			clockService = Clock.getClockService(VectorClock.factory);
			((VectorClock) clockService).initializeTimeStamps(processNo.value, processCount);
			break;
		default:
			System.err.println("SET CLOCK SERVICE ERROR. LOGGER SERVER MAY FAIL TO SET UP");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void parseConfigurationFile() throws IOException {
		setProcessNo();
		configurationFile = new File("D:\\Dropbox\\" + configuration_filename);
		lastModifiedTime = configurationFile.lastModified();
		InputStream input = new FileInputStream("D:\\Dropbox\\" + configuration_filename);
		Yaml yaml = new Yaml();
		Object data = yaml.load(input);
		input.close();
		networkTable = (LinkedHashMap) data; // get the information
		configList = (ArrayList<LinkedHashMap<String, String>>) networkTable.get("configuration");
		sendRuleList = (ArrayList<LinkedHashMap<String, String>>) networkTable.get("sendRules");
		receiveRuleList = (ArrayList<LinkedHashMap<String, String>>) networkTable.get("receiveRules");
		multicastGroupList = (ArrayList<LinkedHashMap<String,String>>) networkTable.get("groups");
		for(Map m : multicastGroupList){
			String groupNameString = (String)m.get("name");
			String regex = "[^0-9]";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(groupNameString);
			int groupNo = Integer.parseInt(matcher.replaceAll("").trim());
			ArrayList<String> memberList = (ArrayList<String>)m.get("members");
			multicast.groupMap.put(groupNo, memberList);
		}
//		System.out.println("INFO: group map:" + multicast.groupMap.toString());
		for(int x : multicast.groupMap.keySet()){
			System.out.println(x + " " + multicast.groupMap.get(x).toString());
		}
		// System.out.println("INFO: " + sendRuleList.toString());
		// System.out.println("INFO: " + receiveRuleList.toString());
//		System.out.println("INFO: " + multicastGroupList.toString());
		this.processCount = configList.size();
		System.out.println("INFO: The number of processes in configuration file: " + this.processCount);
		for (Map m : configList) {
			String name = (String) m.get("name");
			String ip = (String) m.get("ip");
			int port = (int) m.get("port");
			nodeMap.put(name, new Node(ip, port));
		}
		multicast.initVectorMap();
		multicast.initSendingBufferList();
		multicast.initHoldBackQueueList();
		int portNumber = nodeMap.get(local_name).port;
		serverSocket = new ServerSocket(portNumber);
		startListenerThread();
	}

	public void setProcessNo() {
		switch (this.local_name.toLowerCase()) {
		case "alice":
			this.processNo = ProcessNo.ALICE;
			break;
		case "bob":
			this.processNo = ProcessNo.BOB;
			break;
		case "charlie":
			this.processNo = ProcessNo.CHARLIE;
			break;
		case "daphnie":
			this.processNo = ProcessNo.DAPHNIE;
			break;
		case "logger":
			this.processNo = ProcessNo.LOGGER;
			break;
		default:
			System.err.println("Unknown Process!");

		}
	}

	public void startListenerThread() throws IOException {
		Thread listenerThread = new ListenerThread(this);
		listenerThread.start();
	}

	public MessagePasser(String configuration_filename, String local_name)
			throws IOException {
		this.configuration_filename = configuration_filename;
		this.local_name = local_name;
		parseConfigurationFile();
	}

	void reconfiguration() throws IOException, InterruptedException {
		if (configurationFile.lastModified() > lastModifiedTime) {
			lastModifiedTime = configurationFile.lastModified();
			// System.out.println("INFO: " + "configuration file modified!!!");
			nodeMap.clear();
			// System.out.println("INFO: " + "nodeMap cleared! "+ nodeMap.toString());
			// socketMap.clear();
			// System.out.println("INFO: " + "socketMap cleared! "+ socketMap.toString());
			streamMap.clear();
			// System.out.println("INFO: " + "streamMap cleared! "+ streamMap.toString());
			configList.clear();
			sendRuleList.clear();
			receiveRuleList.clear();
			// System.out.println("INFO: " + "config and rule list cleared!");
			serverSocket.close();
			// System.out.println("INFO: " + "reparsing new configuration file!");
			parseConfigurationFile();
			// System.out.println("INFO: " + "reparsing new configuration file done!");
			// System.out.println("INFO: " + "nodeMap reparsed! "+ nodeMap.toString());
			// System.out.println("INFO: " + "socketMap reparsed! "+ socketMap.toString());
			// System.out.println("INFO: " + "streamMap reparsed! "+ streamMap.toString());
			this.clockType = null;
			clockServiceInit();
		}
	}

	/**
	 * Set up clock type and connect to logger
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("resource")
	void clockServiceInit() throws UnknownHostException, IOException,
	InterruptedException {
		if (this.clockType == null) { // clock type is not yet set by logger
			// send request to logger
			if (!this.streamMap.containsKey("logger")) { // not connect yet
				try {
					Socket destSocket = new Socket(InetAddress.getByName(nodeMap.get("logger").ip), nodeMap.get("logger").port);
					ObjectOutputStream oos = new ObjectOutputStream(destSocket.getOutputStream());
					streamMap.put("logger", oos);
					System.out.println("INFO: " + "streamMap updated! " + streamMap.toString());
				}
				catch (ConnectException e) {
					System.err.println("CANNOT CONNECT TO LOGGER");
					return;
				}
			}
			TimeStampedMessage clockSetRequest = new TimeStampedMessage("logger", "clock_set_request", null, null);
			clockSetRequest.set_source(local_name);
			// send it
			ObjectOutputStream oos = this.streamMap.get("logger");
			System.out.println("INFO: " + "sending clock set request");
			oos.writeObject(clockSetRequest);
			oos.flush();
			oos.reset();
			System.out.println("INFO: " + "clock set request sent");
			// wait the logger's set up message
			System.out.println("INFO: " + "Wait for logger's response for 1 sec.");
			Thread.sleep(1000);
			if (this.clockType != null) {
				System.out.println("INFO: " + "clock type set as: " + this.clockType);
				setClockService(this.clockType);
				System.out.println("INFO: " + "clock service initialized");
				if (this.clockType == ClockType.LOGICAL && (this.function == Function.SEND)) {
					((LogicalClock) this.clockService).ticks();
					System.out.println("INFO: " + "logical time stamp now: " + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
				}
				if (this.clockType == ClockType.VECTOR && (this.function == Function.SEND)) {
					((VectorClock) this.clockService).ticks();
					System.out.println("INFO: " + "vector time stamp now: " + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
				}

			} else {
				System.err.println("NO RESPONSE FROM LOGGER");
				return;
			}
		}
	}

	void send(Message message) throws UnknownHostException, IOException,
	InterruptedException {
		System.out.println("SEND MESSAGE(BEFORE RULES) TO: " + message.destination);
		reconfiguration();
		if(this.function != Function.MULTICAST){
			this.function = Function.SEND;
		}
		if (this.clockType == ClockType.LOGICAL) {
			((LogicalClock) this.clockService).ticks();
			System.out.println("INFO: " + "logical time stamp now: " + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
		}
		if (this.clockType == ClockType.VECTOR) {
			((VectorClock) this.clockService).ticks();
			System.out.println("INFO: " + "vector time stamp now: " + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
		}

		try {
			clockServiceInit();
		}
		catch (SocketException e) {
			System.err.println("CANNOT CONNECT TO LOGGER");
			return;
		}

		// System.out.println("INFO: " + "sending..................");
		message.set_action(checkSendingRules(message));

		if (this.clockType == null) {
			if (this.log) {
				System.out.println("INFO: This regular message will not be logged!");
			}
		} else {
			TimeStampedMessage tsm = new TimeStampedMessage(message.destination, message.kind, message.data, this.clockType);
			tsm.set_source(message.source);
			tsm.set_action(message.action);
			tsm.duplicate = message.duplicate;
			tsm.sequenceNumber = message.sequenceNumber;
			tsm.groupNo = message.groupNo;
			tsm.multicast = message.multicast;
			tsm.multicastVector = message.multicastVector;

			if (this.clockType == ClockType.LOGICAL) {
				tsm.setLogicalTimeStamps(((LogicalClock) this.clockService).internalLogicalClock);
			}
			if (this.clockType == ClockType.VECTOR) {
				tsm.setVectorTimeStamps(((VectorClock) this.clockService).internalVectorClock);
			}
			if (this.log) {
				logEvent(tsm, this.function);
				this.log = false;
			}
		}

		switch (message.action) {
		case "drop":
			// do nothing, just drop it
			break;
		case "duplicate":
			sendMessage(message);
			message.set_duplicate();
			sendMessage(message);
			message.duplicate = false;
			break;
		case "delay":
			System.out.println("DELAY!: " + message.destination);
			delaySendingQueue.offer(message);
			break;
		default:
			sendMessage(message);
			break;
		}
	}

	@SuppressWarnings("resource")
	void sendMessage(Message message) throws IOException {
		int seqNo = message.sequenceNumber;
		if (!streamMap.containsKey(message.destination)) {
			System.out.println("INFO: " + "new socket: " + nodeMap.get(message.destination).ip + " " + nodeMap.get(message.destination).port);
			if (!nodeMap.containsKey(message.destination)) {
				System.err.println("Can't find this node in configuration file!");
				return;
			}
			try {
				Socket destSocket = new Socket(InetAddress.getByName(nodeMap.get(message.destination).ip), nodeMap.get(message.destination).port);
				ObjectOutputStream oos = new ObjectOutputStream(destSocket.getOutputStream());
				streamMap.put(message.destination, oos);
			}
			catch (IOException e) {
				System.err.println("Connection Fail!");
				return;
			}
		}
		if (this.clockType == null) {
			System.out.println("INFO: " + "Message without time stamp will be sent!");
			streamMap.get(message.destination).writeObject(message);
			streamMap.get(message.destination).flush();
			streamMap.get(message.destination).reset();
		} else {
			System.out.println("INFO: " + "Time stamped message will be sent!");
			TimeStampedMessage tsm = new TimeStampedMessage(message.destination, message.kind, message.data, this.clockType);
			System.out.println("TSM: " + tsm.destination);
			tsm.set_source(message.source);
			tsm.set_action(message.action);
			tsm.duplicate = message.duplicate;
			tsm.sequenceNumber = message.sequenceNumber;
			tsm.setGroupNo(message.getGroupNo());
			tsm.setMulticastVector(message.getMulticastVector());
			if (this.clockType == ClockType.LOGICAL) {
				tsm.setLogicalTimeStamps(((LogicalClock) this.clockService).internalLogicalClock);
			}
			if (this.clockType == ClockType.VECTOR) {
				tsm.setVectorTimeStamps(((VectorClock) this.clockService).internalVectorClock);
			}
			
			if(message.multicast){
				tsm.setMulticast();
				tsm.setMulticastVector(message.getMulticastVector());
			}
			System.out.println("SEND MESSAGE TO: " + tsm.destination);
			streamMap.get(message.destination).writeObject(tsm);
			streamMap.get(message.destination).flush();
			streamMap.get(message.destination).reset();
		}
		while (!delaySendingQueue.isEmpty() && seqNo != delaySendingQueue.peek().sequenceNumber) {
			System.out.println("PEEK: " + delaySendingQueue.peek().destination);
			sendMessage(delaySendingQueue.poll());
		}
	}

	Message receive() throws IOException, InterruptedException {

		reconfiguration();
		this.function = Function.RECEIVE;

		try {
			clockServiceInit();
		}
		catch (SocketException e) {
			System.err.println("CANNOT CONNECT TO LOGGER");
		}

		receiveMessage();

		if (!popReceivingQueue.isEmpty()) {
			Message popMessage = popReceivingQueue.poll();

			if (popMessage.getClass().equals(TimeStampedMessage.class)) {

				if (((TimeStampedMessage) popMessage).getClockType() == ClockType.LOGICAL) {
					int maxTimeStamp = Math.max(((LogicalClock) this.clockService).internalLogicalClock.timeStamp, ((TimeStampedMessage) popMessage).getLogicalTimeStamps().timeStamp);
					((LogicalClock) this.clockService).internalLogicalClock.timeStamp = maxTimeStamp;
				}

				if (((TimeStampedMessage) popMessage).getClockType() == ClockType.VECTOR) {
					for (int i = 0; i < this.processCount; i++) {
						if (i != this.processNo.value) {
							int maxTimeStamp = Math.max(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix[i], ((TimeStampedMessage) popMessage).getVectorTimeStamps().timeStampMatrix[i]);
							((VectorClock) this.clockService).internalVectorClock.timeStampMatrix[i] = maxTimeStamp;
						}
					}
				}
			}

			if (this.clockType == ClockType.LOGICAL) {
				((LogicalClock) this.clockService).ticks();
				System.out.println("INFO: " + "logical time stamp now: " + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
			}
			if (this.clockType == ClockType.VECTOR) {
				((VectorClock) this.clockService).ticks();
				System.out.println("INFO: " + "vector time stamp now: " + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
			}

			if (this.log) {
				logEvent(popMessage, this.function);
				this.log = false;
			}
			return popMessage;
		} else {
			TimeStampedMessage emptyMessage = new TimeStampedMessage("logger", this.function.toString(), "No message to receive!", this.clockType);
			emptyMessage.set_source(local_name);
			if (this.clockType == ClockType.LOGICAL) {
				((LogicalClock) this.clockService).ticks();
				System.out.println("INFO: " + "logical time stamp now: " + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
			}
			if (this.clockType == ClockType.VECTOR) {
				((VectorClock) this.clockService).ticks();
				System.out.println("INFO: " + "vector time stamp now: " + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
			}
			if (this.log) {
				logEvent(emptyMessage, this.function);
				this.log = false;
			}
			return new Message(null, null, "No message to receive!");
		}

	}

	void receiveMessage() {
		Message receivedMessage;
		// System.out.println("INFO: " + "Receiving..................");
		if (!messageQueue.isEmpty()) {
			receivedMessage = messageQueue.poll();
			String action = checkReceivingRules(receivedMessage);
			switch (action) {
			case "drop":
				// System.out.println("INFO: " + "receive: drop");
				// do nothing, just drop it
				// System.out.println("INFO: " + "receive: drop");
				break;
			case "duplicate":
				// System.out.println("INFO: " + "receive: duplicate");
				popReceivingQueue.offer(receivedMessage);
				popReceivingQueue.offer(receivedMessage);
				while (!delayReceivingQueue.isEmpty()) {
					popReceivingQueue.offer(delayReceivingQueue.poll());
				}
				break;
			case "delay":
				// System.out.println("INFO: " + "receive: delay");
				delayReceivingQueue.offer(receivedMessage);
				receiveMessage();
				break;
			default:
				// default action
				// System.out.println("INFO: " + "receive: default");
				popReceivingQueue.offer(receivedMessage);
				while (!delayReceivingQueue.isEmpty()) {
					popReceivingQueue.offer(delayReceivingQueue.poll());
				}
			}
		}
		// System.out.println("INFO: " + "Receiving done..................");
	}

	@SuppressWarnings("rawtypes")
	String checkSendingRules(Message message) {

		for (Map m : sendRuleList) {

			boolean srcMatch = false;
			boolean dstMatch = false;
			boolean seqMatch = false;
			boolean kindMatch = false;
			boolean duplicate = false;

			if (!m.containsKey("src")) {
				srcMatch = true;
			} else if (((String) m.get("src")).equalsIgnoreCase(message.source)) {
				srcMatch = true;
			}

			if (!m.containsKey("dest")) {
				dstMatch = true;
			} else if (((String) m.get("dest")).equalsIgnoreCase(message.destination)) {
				dstMatch = true;
			}

			if (!m.containsKey("seqNum")) {
				seqMatch = true;
			} else if ((int) m.get("seqNum") == message.sequenceNumber) {
				seqMatch = true;
			}

			if (!m.containsKey("kind")) {
				kindMatch = true;
			} else if (((String) m.get("kind")).equalsIgnoreCase(message.kind)) {
				kindMatch = true;
			}

			if (!m.containsKey("duplicate")) {
				duplicate = true;
			} else if (m.get("duplicate").equals(message.duplicate)) {
				duplicate = true;
			}

			if (srcMatch && dstMatch && seqMatch && kindMatch && duplicate) {
				return (String) m.get("action");
			}
		}
		return "none";
	}

	@SuppressWarnings("rawtypes")
	String checkReceivingRules(Message message) {

		for (Map m : receiveRuleList) {

			boolean srcMatch = false;
			boolean dstMatch = false;
			boolean seqMatch = false;
			boolean kindMatch = false;
			boolean duplicate = false;

			if (!m.containsKey("src")) {
				srcMatch = true;
			} else if (((String) m.get("src")).equalsIgnoreCase(message.source)) {
				srcMatch = true;
			}

			if (!m.containsKey("dest")) {
				dstMatch = true;
			} else if (((String) m.get("dest")).equalsIgnoreCase(message.destination)) {
				dstMatch = true;
			}

			if (!m.containsKey("seqNum")) {
				seqMatch = true;
			} else if ((int) m.get("seqNum") == message.sequenceNumber) {
				seqMatch = true;
			}

			if (!m.containsKey("kind")) {
				kindMatch = true;
			} else if (((String) m.get("kind")).equalsIgnoreCase(message.kind)) {
				kindMatch = true;
			}

			if (!m.containsKey("duplicate")) {
				duplicate = true;
			} else if (m.get("duplicate").equals(message.duplicate)) {
				duplicate = true;
			}

			if (srcMatch && dstMatch && seqMatch && kindMatch && duplicate) {
				return (String) m.get("action");
			}

		}
		return "none";
	}

	void logEvent(Message logMessage, Function event) throws IOException {
		System.out.println("INFO: LOG THIS " + event);
		TimeStampedMessage timeStampedLog = null;
		TimeStampedMessage sendingLog = null;
		if (logMessage.getClass().equals(TimeStampedMessage.class)) {
			timeStampedLog = (TimeStampedMessage) logMessage;
			if(this.function == Function.MULTICAST){
				timeStampedLog.destination = "Group" + timeStampedLog.groupNo;
				timeStampedLog.multicast = true;
			}
			sendingLog = new TimeStampedMessage("logger", "log" + event, timeStampedLog, null);
			if (this.clockType == ClockType.LOGICAL) {
				sendingLog.setLogicalTimeStamps(((LogicalClock) this.clockService).internalLogicalClock);
			}
			if (this.clockType == ClockType.VECTOR) {
				sendingLog.setVectorTimeStamps(((VectorClock) this.clockService).internalVectorClock);
			}
			sendingLog.set_source(local_name);
			ObjectOutputStream oos = this.streamMap.get("logger");
			oos.writeObject(sendingLog);
			oos.flush();
			oos.reset();
		} else {
			System.out.println("INFO: This message cannot be logged!");
		}
	}

	void retrieveLog() throws IOException, InterruptedException {
		reconfiguration();
		// set up the request message with kind "retrieve"
		TimeStampedMessage retrieve = new TimeStampedMessage("logger", "retrieve", null, null);

		retrieve.set_source(this.local_name);

		this.function = Function.RETRIEVE;

		try {
			clockServiceInit();
		}
		catch (SocketException e) {
			System.err.println("CANNOT CONNECT TO LOGGER");
			return;
		}

		if (this.clockType == ClockType.LOGICAL) {
			((LogicalClock) this.clockService).ticks();
			System.out.println("INFO: " + "logical time stamp now: " + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
		}
		if (this.clockType == ClockType.VECTOR) {
			((VectorClock) this.clockService).ticks();
			System.out.println("INFO: " + "vector time stamp now: " + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
		}

		if (this.clockType == ClockType.LOGICAL) {
			retrieve.setLogicalTimeStamps(((LogicalClock) this.clockService).internalLogicalClock);
		}
		if (this.clockType == ClockType.VECTOR) {
			retrieve.setVectorTimeStamps(((VectorClock) this.clockService).internalVectorClock);
		}
		// log first
		if (this.log) {
			logEvent(retrieve, this.function);
			this.log = false;
		}
		// send the request message to the logger
		ObjectOutputStream oos = this.streamMap.get("logger");
		oos.writeObject(retrieve);
		oos.flush();
		oos.reset();
		System.out.println("INFO: wait logger for 1 sec");
		Thread.sleep(1000);
		// receive();
		// receiveMessage();
		if (!logQueue.isEmpty()) {
			Message logMessage = logQueue.poll();
			System.out.println(logMessage.data.toString());
		}
	}

	void printTimeStamp() throws IOException, InterruptedException {
		this.function = Function.PRINTSTAMP;
		TimeStampedMessage tsm = new TimeStampedMessage("logger", "printTimeStamp", null, null);
		// add source to print log
		tsm.set_source(this.local_name);

		try {
			clockServiceInit();
		}
		catch (SocketException e) {
			System.err.println("CANNOT CONNECT TO LOGGER");
			return;
		}
		// ticks
		if (this.clockType == ClockType.LOGICAL) {
			((LogicalClock) this.clockService).ticks();
			System.out.println("INFO: " + "logical time stamp now: " + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
		}
		if (this.clockType == ClockType.VECTOR) {
			((VectorClock) this.clockService).ticks();
			System.out.println("INFO: " + "vector time stamp now: " + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
		}

		if (this.clockType == ClockType.LOGICAL) {
			System.out.println("Clock type: " + this.clockType + "; Time Stamp:" + ((LogicalClock) this.clockService).internalLogicalClock.timeStamp);
			if (this.log) {
				logEvent(tsm, this.function);
				this.log = false;
			}
		} else if (this.clockType == ClockType.VECTOR) {
			System.out.println("Clock type: " + this.clockType + "; Time Stamp:" + Arrays.toString(((VectorClock) this.clockService).internalVectorClock.timeStampMatrix));
			if (this.log) {
				logEvent(tsm, this.function);
				this.log = false;
			}
		} else {
			System.err.println("CLOCK TYPE IS NOT SET YET. CANNOT PRINT TIME STAMP");
		}

	}
}

enum ProcessNo {
	LOGGER(0), ALICE(1), BOB(2), CHARLIE(3), DAPHNIE(4);

	public int value;
	public static int getProcessNo(String processName) {
		switch(processName.toLowerCase()){
		case "logger":
			return LOGGER.value;
		case "alice":
			return ALICE.value;
		case "bob":
			return BOB.value;
		case "charlie":
			return CHARLIE.value;
		case "daphnie":
			return DAPHNIE.value;
		default:
			return -1;
		}
	}
	
	public static String getProcessName(int processNo){
		switch(processNo){
		case 0:
			return "logger";
		case 1:
			return "alice";
		case 2:
			return "bob";
		case 3:
			return "charlie";
		case 4:
			return "daphnie";
		default:
			return null;
		}
	}
	
	private ProcessNo(int value) {
		this.value = value;
	}
}

enum Function {
	SEND, RECEIVE, RETRIEVE, PRINTSTAMP, MULTICAST;
}
