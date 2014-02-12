import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collections;

public class ReadInputStream extends Thread {

	ObjectInputStream ois;

	MessagePasser messagePasser;

	public ReadInputStream(Socket clientSocket, MessagePasser messagePasser)
			throws IOException {
		ois = new ObjectInputStream(clientSocket.getInputStream());
		this.messagePasser = messagePasser;
	}

	@SuppressWarnings("resource")
	public void run() {
		while (true) {
			try {
				Message receivedMessage = (Message) ois.readObject();
				// if the message is set clock from logger, set the clock and don't enqueue it
				if (receivedMessage.kind.equals("set_clock") && receivedMessage.source.equals("logger")) {
					messagePasser.clockType = ((TimeStampedMessage) receivedMessage).getClockType();
					messagePasser.setClockService(messagePasser.clockType);
					System.out.println("INFO: " + "clock type set as " + messagePasser.clockType);
					continue;
				}
				// the log_reply message replying for nodes' log retrieve request
				if (receivedMessage.kind.equalsIgnoreCase("log_reply") && receivedMessage.source.equals("logger")) {
					this.messagePasser.logQueue.offer((TimeStampedMessage) receivedMessage);
					continue;
				}
				if (!messagePasser.streamMap.containsKey(receivedMessage.source)) {
					// add the stream in the stream map
					System.out.println("INFO: " + "call back");
					System.out.println("before dead: " + receivedMessage.source);
					Node callBackNode = messagePasser.nodeMap.get(receivedMessage.source);
					Socket callBackSocket = new Socket(InetAddress.getByName(callBackNode.ip), callBackNode.port);
					ObjectOutputStream oos = new ObjectOutputStream(callBackSocket.getOutputStream());
					messagePasser.streamMap.put(receivedMessage.source, oos);
				}
				if(receivedMessage.kind.equalsIgnoreCase("NACK")){
					System.out.println("RETRANSMIT!");
					messagePasser.multicast.retransmit(receivedMessage);
					continue;
				}
				if(receivedMessage.multicast){
					System.out.println("INFO: before deliver: " + receivedMessage.getGroupNo());
					messagePasser.multicast.deliver(receivedMessage);
					continue;
				}

				this.messagePasser.messageQueue.offer(receivedMessage);
			}
			catch (SocketException e) {
				System.err.println("Remote socket down.");
				break;
			}
			catch (IOException e) {
				System.err.println("Remote socket down.");
				break;
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}

class LoggerReadInputStream extends Thread {

	ObjectInputStream ois;

	LoggerMessagePasser loggerMessagePasser;

	public LoggerReadInputStream(Socket clientSocket,
			LoggerMessagePasser loggerMessagePasser) throws IOException {
		ois = new ObjectInputStream(clientSocket.getInputStream());
		this.loggerMessagePasser = loggerMessagePasser;
	}

	@SuppressWarnings("resource")
	public void run() {
		while (true) {
			try {
				TimeStampedMessage receivedTimeStampedMessage = (TimeStampedMessage) ois.readObject();
				System.out.println("INFO: " + "logger gets a timestamped message!");
				if (!loggerMessagePasser.streamMap.containsKey(receivedTimeStampedMessage.source) || receivedTimeStampedMessage.kind.equalsIgnoreCase("clock_set_request")) {
					//System.out.println("INFO: " + "logger call back");
					Node callBackNode = loggerMessagePasser.nodeMap.get(receivedTimeStampedMessage.source);
					Socket callBackSocket = new Socket(InetAddress.getByName(callBackNode.ip), callBackNode.port);
					ObjectOutputStream oos = new ObjectOutputStream(callBackSocket.getOutputStream());
					loggerMessagePasser.streamMap.put(receivedTimeStampedMessage.source, oos);
					//System.out.println("INFO: logger's stream map updated! " + loggerMessagePasser.streamMap.toString());
					// tell the node the clock type
					TimeStampedMessage setClockMessage;
					if (loggerMessagePasser.clockType == ClockType.LOGICAL) {
						// send back time stamps information of logical clock
						System.out.println("INFO: " + "Set your clock LOGICAL!");
						setClockMessage = new TimeStampedMessage(receivedTimeStampedMessage.source, "set_clock", null, ClockType.LOGICAL);
						setClockMessage.set_source(loggerMessagePasser.local_name);
						oos.writeObject(setClockMessage);
						oos.flush();
						oos.reset();
					}
					// send back time stamps information of vector clock
					else if (loggerMessagePasser.clockType == ClockType.VECTOR) {
						System.out.println("INFO: " + "Set your clock VECTOR!");
						setClockMessage = new TimeStampedMessage(receivedTimeStampedMessage.source, "set_clock", null, ClockType.VECTOR);
						setClockMessage.set_source(loggerMessagePasser.local_name);
						oos.writeObject(setClockMessage);
						oos.flush();
						oos.reset();
					} else {
						System.out.println("INFO: " + "logger's clock is not set yet");
					}
				}

				else if (receivedTimeStampedMessage.kind.substring(0, 3).equalsIgnoreCase("log")) {
					//System.out.println("INFO: Logger received a log");
					if (loggerMessagePasser.clockType == ClockType.LOGICAL) {
						LogicalLog logicalLog = new LogicalLog(receivedTimeStampedMessage);
						loggerMessagePasser.logicalLogList.add(logicalLog);
					}
					if (loggerMessagePasser.clockType == ClockType.VECTOR) {
						VectorLog vectorLog = new VectorLog(receivedTimeStampedMessage);
						loggerMessagePasser.vectorLogList.add(vectorLog);
					}
				} else {
					// if it's a retrieve log request
					if (receivedTimeStampedMessage.kind.equalsIgnoreCase("retrieve")) {
						
						String reply = loggerMessagePasser.print();
						TimeStampedMessage logReply = new TimeStampedMessage(receivedTimeStampedMessage.source, "log_reply", reply.toString(), null);
						logReply.set_source("logger");
						ObjectOutputStream oos = loggerMessagePasser.streamMap.get(receivedTimeStampedMessage.source);
						oos.writeObject(logReply);
						oos.flush();
						oos.reset();
					}
				}
			}
			catch (SocketException e) {
				System.err.println("Remote socket down.");
				break;
			}
			catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IOException e) {
				System.err.println("Remote socket down.");
				break;
			}

		}
	}
}
