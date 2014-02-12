import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	String source = "***";

	String destination = "***";

	String kind = "***";

	Object data = "***";

	int sequenceNumber = -1;

	String action = "***";

	boolean duplicate = false;

	protected boolean multicast = false;

	protected int[] multicastVector = {};

	protected int groupNo = -1;
	
	public Message clone(Message oldMsg){
		Message retmsg = new Message(oldMsg.destination, oldMsg.kind, oldMsg.data);
		retmsg.source = oldMsg.source;
		retmsg.action = oldMsg.action;
		retmsg.duplicate = oldMsg.duplicate;
		retmsg.groupNo = oldMsg.groupNo;
		retmsg.multicast = oldMsg.multicast;
		retmsg.multicastVector = oldMsg.multicastVector;
		retmsg.sequenceNumber = oldMsg.sequenceNumber;
		return retmsg;
	}
	
	public void setMulticastVector(int[] newMulticastVector){
		
//		this.multicastVector = new int[newMulticastVector.length];
//		for(int i=0; i<newMulticastVector.length; i++ ){
//			this.multicastVector[i] = newMulticastVector[i];
//		}
		
		this.multicastVector = newMulticastVector;
	}
	
	public int[] getMulticastVector(){
		return this.multicastVector;
	}

	public void setMulticast(){
		this.multicast = true;
	}

	public int getGroupNo(){
		return this.groupNo;
	}
	
	public void setGroupNo(int groupNo){
		this.groupNo = groupNo;
	}

	public Message(String dest, String kind, Object data) {
		if (dest != null) {
			destination = dest;
		}
		if (kind != null) {
			this.kind = kind;
		}
		if (data != null) {
			this.data = data;
		}
		Pattern pattern = Pattern.compile("[^0-9]");
		Matcher matcher = pattern.matcher(this.destination);
		if(!matcher.replaceAll("".trim()).equals("")){
			this.groupNo = Integer.parseInt(matcher.replaceAll("".trim()));
		}
	}

	public void set_source(String source) {
		this.source = source;
	}

	public void set_seqNum(int sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public void set_duplicate() {
		this.duplicate = true;
	}

	public void set_action(String action) {
		this.action = action;
	}

	public String toString() {
		return "[source=" + source + "; destination=" + destination + "; kind=" + kind + "; data=" + (String) data + "; seqNum=" + sequenceNumber + "; action=" + action + "; duplicate=" + duplicate + "]";
	}
}

class TimeStampedMessage extends Message {

	private static final long serialVersionUID = 1L;

	private ClockType clockType;

	private LogicalTimeStamps logicalTimeStamps;

	private VectorTimeStamps vectorTimeStamps;

	public TimeStampedMessage(String dest, String kind, Object data,
			ClockType clockType) {
		super(dest, kind, data);
		this.clockType = clockType;

	}

	public ClockType getClockType() {
		return clockType;
	}

	public LogicalTimeStamps getLogicalTimeStamps() {
		return this.logicalTimeStamps;
	}

	public void setLogicalTimeStamps(LogicalTimeStamps lts) {
		this.logicalTimeStamps = lts;
	}

	public VectorTimeStamps getVectorTimeStamps() {
		return this.vectorTimeStamps;
	}

	public void setVectorTimeStamps(VectorTimeStamps vts) {
		this.vectorTimeStamps = vts;
	}

	public String toString() {
		switch (this.clockType) {
		case LOGICAL:
			return "[source=" + source + "; destination=" + destination + "; kind=" + kind + "; data=" + (String) data + "; seqNum=" + sequenceNumber + "; action=" + action + "; duplicate=" + duplicate + "; processNo=" + logicalTimeStamps.processNo + "; logicalTimeStamp=" + logicalTimeStamps.timeStamp + "; multicast=" + multicast + "; groupNo=" + groupNo + "; multicastVector=" + Arrays.toString(this.multicastVector) + "]";
		case VECTOR:
			return "[source=" + source + "; destination=" + destination + "; kind=" + kind + "; data=" + (String) data + "; seqNum=" + sequenceNumber + "; action=" + action + "; duplicate=" + duplicate + "; vectorTimeStamp=" + Arrays.toString(vectorTimeStamps.timeStampMatrix) + "; multicast=" + multicast + "; groupNo=" + groupNo + "; multicastVector=" + Arrays.toString(this.multicastVector) + "]";

		default:
			return "TIME STAMP MESSAGE ERROR";
		}
	}
}

