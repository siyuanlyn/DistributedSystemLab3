
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;

public class Multicast {
	HashMap<Integer, ArrayList<String>> groupMap = new HashMap<>();
	HashMap<Integer, Integer[]> vectorMap = new HashMap<>();
	MessagePasser messagePasser;
	ArrayList<LinkedList<Message>> holdBackQueueList = new ArrayList<>();
	ArrayList<ArrayList<Message>> sendingBufferList = new ArrayList<>();

	public Multicast(MessagePasser messagePasser){
		this.messagePasser = messagePasser;
		System.out.println("INFO: initialize the multicast!");

	}

	public void initSendingBufferList(){
		System.out.println("INFO: GROUP MAP SIZE: " + groupMap.size());
		for(int i=0; i<groupMap.size(); i++){
			sendingBufferList.add(new ArrayList<Message>());
		}
	}
	
	public void initHoldBackQueueList(){
		for(int i=0; i<groupMap.size(); i++){
			holdBackQueueList.add(new LinkedList<Message>());
		}
	}
	
	public void initVectorMap(){
		int vectorLength = messagePasser.nodeMap.size();
		for(Integer i : groupMap.keySet()){
			Integer[] groupVector = new Integer[vectorLength];
			for(int j=0; j<vectorLength; j++){
				groupVector[j] = 0;
			}
			vectorMap.put(i, groupVector);
			System.out.println("INFO: initialize vector map: " + Arrays.toString(vectorMap.get(i)));
		}
	}

	public void send(Message message) throws IOException, InterruptedException{
		int groupNo = message.getGroupNo();
		messagePasser.function = Function.MULTICAST;
		//b-multicast:
		System.out.println("INFO: vector map: " + vectorMap.toString());
		System.out.println("INFO: group number: " + message.getGroupNo());
		vectorMap.get(message.getGroupNo())[messagePasser.processNo.value]++;
		message.setMulticast();
		
		int length = (vectorMap.get(message.getGroupNo())).length;
		int[] tmp = new int[length];
		for(int i=0; i<length; i++){
			tmp[i] =  (vectorMap.get(message.getGroupNo()))[i];
		}
		
		message.setMulticastVector(tmp);
		//save every multicasting message in the buffer for retransmission
		for(String dest : groupMap.get(message.getGroupNo())){
			if(!dest.equalsIgnoreCase(messagePasser.local_name)){
				message.destination = dest;
				System.out.println("INFO: MULTISEND MULTICAST VECTOR: " + Arrays.toString(message.multicastVector));
				Message sendingMsg = message.clone(message);
				
				messagePasser.send(sendingMsg);
				if(messagePasser.clockType == ClockType.VECTOR){
					((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
				}	
			}
		}
		System.out.println("BEFORE SENDING BUFFER ENQUEUE: " + Arrays.toString(message.multicastVector));
		for(Message m : sendingBufferList.get(groupNo-1)){
			System.out.println("BUFFFFFFFFFFFFFFER before add: " + Arrays.toString(m.multicastVector));
		}
		sendingBufferList.get(groupNo-1).add(message);
		for(Message m : sendingBufferList.get(groupNo-1)){
			System.out.println("BUFFFFFFFFFFFFFFER after add: " + Arrays.toString(m.multicastVector));
		}
		if(messagePasser.clockType == ClockType.VECTOR){
			((VectorClock)messagePasser.clockService).ticks();
		}
	}

	//r-deliver
	public void deliver(Message message) {
		//check time stamp;
		int[] messageTimeStamp = message.multicastVector;
		int groupNo = message.getGroupNo();
		int length = (vectorMap.get(message.getGroupNo())).length;
		int[] internalMulticastTimeStamp = new int[length];
		for(int i=0; i<length; i++){
			internalMulticastTimeStamp[i] =  (vectorMap.get(message.getGroupNo()))[i];
		}
		
		System.out.println("INFO: Deliver, disposal: " + message.getGroupNo() + " " + ProcessNo.getProcessNo(message.source) + " " + Arrays.toString(messageTimeStamp) + " " + Arrays.toString(internalMulticastTimeStamp));
		Disposal disposal = compareMulticastTimeStamp(message.getGroupNo(), ProcessNo.getProcessNo(message.source), messageTimeStamp, internalMulticastTimeStamp, message.duplicate);
		if(disposal.discard){
			//do nothing;
			System.out.println("DISCARD");
		}
		else if(disposal.holdBack){
			System.out.println("HOLDBACK");
			this.holdBackQueueList.get(groupNo-1).offer(message);
			//sort the hold back queue;
			Collections.sort(this.holdBackQueueList.get(groupNo-1), new holdBackComparator());
		}
		else{
			//deliver:
			System.out.println("DELIVER!");
			if(!message.duplicate){
				++vectorMap.get(message.getGroupNo())[ProcessNo.getProcessNo(message.source)];
			}
			messagePasser.messageQueue.offer(message);
			internalMulticastTimeStamp = new int[length];
			for(int i=0; i<length; i++){
				internalMulticastTimeStamp[i] =  (vectorMap.get(message.getGroupNo()))[i];
			}
			while(this.holdBackQueueList.get(groupNo-1).size() !=0){
				System.out.println("HOLDBACK DEQUEUE!");
				System.out.println("INFO: Deliver, disposal 2: " + message.getGroupNo() + " " + ProcessNo.getProcessNo(this.holdBackQueueList.get(groupNo-1).peek().source) + " " + Arrays.toString(this.holdBackQueueList.get(groupNo-1).peek().multicastVector) + " " + Arrays.toString(internalMulticastTimeStamp));
				Disposal redisposal = compareMulticastTimeStamp(message.getGroupNo(), ProcessNo.getProcessNo(this.holdBackQueueList.get(groupNo-1).peek().source), this.holdBackQueueList.get(groupNo-1).peek().multicastVector, internalMulticastTimeStamp, message.duplicate);
				if(!redisposal.holdBack){
					messagePasser.messageQueue.offer(this.holdBackQueueList.get(groupNo-1).poll());
					vectorMap.get(message.getGroupNo())[ProcessNo.getProcessNo(message.source)]++;
				}
				else{
					break;
				}
			}
		}

	}

	private Disposal compareMulticastTimeStamp(Integer groupNo, Integer srcNo, int[] external, int[] internal, boolean dup) {
		int sourceNo = srcNo;
		Disposal disposal = new Disposal();
		if(external[sourceNo] <= internal[sourceNo] && dup == false){
			//drop duplicate retransmision;
			System.out.println("discard disposal");
			disposal.discard = true;
		}
		else if(external[sourceNo] - internal[sourceNo] > 1){
			//send NACK(source, timestamp);
			System.out.println("nack(1)");
			sendNACK(groupNo, sourceNo, internal);
			disposal.holdBack = true;	//hold back
		}
		else {
			for(int i=0; i<internal.length; i++){
				if(i != sourceNo && external[i]>internal[i]){
					//send NACK(source, timestamp)
					System.out.println("nack(2)");
					sendNACK(groupNo, i, internal);
					disposal.holdBack = true;
				}
			}
		}
		return disposal;
	}

	private void sendNACK(Integer groupNo, int srcNo, int[] timeStamp){
		//translate srcNo to process name
		System.out.println("N A C K !");
		String processNameString = ProcessNo.getProcessName(srcNo);
		Message message = new Message(processNameString, "NACK", null);
		message.set_source(messagePasser.local_name);
		message.setMulticast();
		message.setGroupNo(groupNo);
		message.setMulticastVector(timeStamp);
		try {
			messagePasser.send(message);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//send
		//self event clock --;
		if(messagePasser.clockType == ClockType.VECTOR){
			((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
		}
	}

	public void retransmit(Message message){
		//get the index in the sending buffer
		int groupNo = message.getGroupNo();
		int retransmitIndex = message.multicastVector[messagePasser.processNo.value]+1;
		System.out.println("RETRANSMIT INDEX!: " + retransmitIndex);
		for(Message m : sendingBufferList.get(groupNo-1)){
			System.out.println("IN THE SENDING BUFFER: " + Arrays.toString(m.multicastVector));
			if(m.multicastVector[messagePasser.processNo.value] == retransmitIndex){
				Message retransmitMsg = m;
				retransmitMsg.destination = message.source;
				try {
					System.out.println("RETRANSMIT: " + Arrays.toString(retransmitMsg.multicastVector));
					System.out.println("RETRANSMIT: " + retransmitMsg.destination);
					messagePasser.send(retransmitMsg);
					if(messagePasser.clockType == ClockType.VECTOR){
						((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			}
			
		}
	}
}

class Disposal {
	boolean holdBack = false;
	boolean discard = false;
}

class holdBackComparator implements Comparator<Message> {

	public int compare(Message msg1, Message msg2) {
		int i;
		for (i = 0; i < msg1.multicastVector.length; i++) {
			if (msg1.multicastVector[i] != msg2.multicastVector[i]) {
				break;
			}
		}
		if (i == msg1.multicastVector.length) {
			return 0; // completely equal!
		} else {
			for (i = 0; i < msg1.multicastVector.length; i++) {
				if (msg1.multicastVector[i] <= msg2.multicastVector[i]) {
					continue;
				}
				break;
			}
			if (i == msg1.multicastVector.length) {
				return -1; // not equal, happen before
			} else {
				for (i = 0; i < msg1.multicastVector.length; i++) {
					if (msg1.multicastVector[i] >= msg2.multicastVector[i]) {
						continue;
					}
					break;
				}
				if (i == msg1.multicastVector.length) {
					return 1; // happen after
				} else {
					return 0; // concurrent
				}
			}
		}

	}
}
