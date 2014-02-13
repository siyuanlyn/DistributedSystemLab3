import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;


public class Mutex {
	LinkedList<TimeStampedMessage> mutexRequestQueue = new LinkedList<>();
	LinkedList<Message> receivedVote = new LinkedList<>();
	MessagePasser messagePasser;
	MutexState state = MutexState.RELEASED;
	boolean voted = false;
	HashMap<String, Message> voteMap = new HashMap<>();
	HashSet<String> voteSet = new HashSet<>();
	public Mutex(MessagePasser messagePasser){
		this.messagePasser = messagePasser;
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			int groupNo = Integer.parseInt(group.substring(5));
			for(int grpNo : messagePasser.multicast.groupMap.keySet()){
				if(grpNo == groupNo){
					for(String groupMember : messagePasser.multicast.groupMap.get(grpNo)){
						voteSet.add(groupMember);
					}
				}
			}
		}
	}

	public void request() throws IOException, InterruptedException{
		if(state == MutexState.HELD){
			System.err.println("Already Holding the Lock!");
			return;
		}
		this.messagePasser.function = Function.REQUEST_MUTEX;
		state = MutexState.WANTED;
		System.out.println("MUTEX LOCK WANTED!");
		int seqNo = messagePasser.generateSeqNum();

		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			TimeStampedMessage request = new TimeStampedMessage(group, "mutex_request", null, this.messagePasser.clockType);
			request.source = this.messagePasser.local_name;
			request.sequenceNumber = seqNo;
			request.groupNo = Integer.parseInt(group.substring(5));

			if (this.messagePasser.clockType == ClockType.LOGICAL) {
				request.setLogicalTimeStamps(((LogicalClock) this.messagePasser.clockService).internalLogicalClock);
			}
			if (this.messagePasser.clockType == ClockType.VECTOR) {
				request.setVectorTimeStamps(((VectorClock) this.messagePasser.clockService).internalVectorClock);
			}
			
			
			messagePasser.multicast.send(request);
			if(this.messagePasser.clockType == ClockType.VECTOR){
				System.out.println("vector clock set back in mutex.request!");
				((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
			}
			if(this.messagePasser.clockType == ClockType.LOGICAL){
				System.out.println("logical clock set back in mutex.request!");
				((LogicalClock)messagePasser.clockService).internalLogicalClock.timeStamp--;
			}
		}
		this.messagePasser.function = Function.REQUEST_MUTEX;
		if(messagePasser.clockType == ClockType.VECTOR){
			((VectorClock)this.messagePasser.clockService).ticks();
			System.out.println("INFO: Vector clock ticks: " + ((VectorClock)this.messagePasser.clockService).internalVectorClock.timeStampMatrix[this.messagePasser.processNo.value]);
		}
		if(messagePasser.clockType == ClockType.LOGICAL){
			((LogicalClock)this.messagePasser.clockService).ticks();
			System.out.println("INFO: Logical clock ticks: " + ((LogicalClock)this.messagePasser.clockService).internalLogicalClock.timeStamp);
		}
		
		System.out.println(this.messagePasser.function);
		if (this.messagePasser.log && this.messagePasser.function == Function.REQUEST_MUTEX) {
			
			TimeStampedMessage logRequestMessage = new TimeStampedMessage("mutex_request", "mutex_request", null, messagePasser.clockType);
			logRequestMessage.source = this.messagePasser.local_name;
			logRequestMessage.sequenceNumber = seqNo;
			if(this.messagePasser.clockType == ClockType.LOGICAL){
				logRequestMessage.setLogicalTimeStamps(((LogicalClock)this.messagePasser.clockService).internalLogicalClock);
			}
			if(this.messagePasser.clockType == ClockType.VECTOR){
				logRequestMessage.setVectorTimeStamps(((VectorClock)this.messagePasser.clockService).internalVectorClock);
			}
			System.out.println("LOG THIS REQUEST!");
			this.messagePasser.logEvent(logRequestMessage, this.messagePasser.function);
			this.messagePasser.log = false;
		}
		new LockWatcher(this).start();
//		this.messagePasser.function = null;
	}

	public void handleRequest(Message request) throws UnknownHostException, IOException, InterruptedException{
		if(request.duplicate){
			return;
		}
		messagePasser.clockServiceInit();
		handleTimeStampedMessage(request);
		if(state == MutexState.HELD || this.voted){
			this.mutexRequestQueue.offer((TimeStampedMessage)request);
			if (this.messagePasser.clockType == ClockType.LOGICAL) {
				Collections.sort(this.mutexRequestQueue, new LogicalTSMComparator());
			}
			if (this.messagePasser.clockType == ClockType.VECTOR) {
				Collections.sort(this.mutexRequestQueue,new VectorTSMComparator());
			}
		}
		else{
			Message response = new Message(request.source, "mutex_vote", null);
			response.sequenceNumber = messagePasser.generateSeqNum();
			response.source = messagePasser.local_name;


			messagePasser.send(response);
			this.voted = true;
		}
	}

	public void release() throws IOException, InterruptedException{
		if(state == MutexState.RELEASED){
			System.err.println("Already Releasing the Lock!");
			return;
		}
		this.messagePasser.function = Function.RELEASE_MUTEX;
		state = MutexState.RELEASED;
		int seqNo = messagePasser.generateSeqNum();
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			TimeStampedMessage release = new TimeStampedMessage(group, "mutex_release", null, this.messagePasser.clockType);
			release.source = this.messagePasser.local_name;
			release.sequenceNumber = seqNo;
			release.groupNo = Integer.parseInt(group.substring(5));

			if (this.messagePasser.clockType == ClockType.LOGICAL) {
				release.setLogicalTimeStamps(((LogicalClock) this.messagePasser.clockService).internalLogicalClock);
			}
			if (this.messagePasser.clockType == ClockType.VECTOR) {
				release.setVectorTimeStamps(((VectorClock) this.messagePasser.clockService).internalVectorClock);
			}
			
			
			messagePasser.multicast.send(release);
			if(this.messagePasser.clockType == ClockType.VECTOR){
				((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
			}
			if(this.messagePasser.clockType == ClockType.LOGICAL){
				((LogicalClock)messagePasser.clockService).internalLogicalClock.timeStamp--;
			}
			handleRelease(release);
		}
		this.messagePasser.function = Function.RELEASE_MUTEX;
		if(messagePasser.clockType == ClockType.VECTOR){
			((VectorClock)this.messagePasser.clockService).ticks();
			System.out.println("INFO: Vector clock ticks: " + ((VectorClock)this.messagePasser.clockService).internalVectorClock.timeStampMatrix[this.messagePasser.processNo.value]);
		}
		if(messagePasser.clockType == ClockType.LOGICAL){
			((LogicalClock)this.messagePasser.clockService).ticks();
			System.out.println("INFO: Logical clock ticks: " + ((LogicalClock)this.messagePasser.clockService).internalLogicalClock.timeStamp);
		}
		
		System.out.println(this.messagePasser.function);
		if (this.messagePasser.log && this.messagePasser.function == Function.RELEASE_MUTEX) {
			System.out.println("LOG THIS RELEASE!");
			TimeStampedMessage logReleaseMessage = new TimeStampedMessage("mutex_release", "mutex_release", null, this.messagePasser.clockType);
			logReleaseMessage.source = this.messagePasser.local_name;
			logReleaseMessage.sequenceNumber = seqNo;
			if(this.messagePasser.clockType == ClockType.LOGICAL){
				logReleaseMessage.setLogicalTimeStamps(((LogicalClock)this.messagePasser.clockService).internalLogicalClock);
			}
			if(this.messagePasser.clockType == ClockType.VECTOR){
				logReleaseMessage.setVectorTimeStamps(((VectorClock)this.messagePasser.clockService).internalVectorClock);
			}

			this.messagePasser.logEvent(logReleaseMessage, this.messagePasser.function);
			this.messagePasser.log = false;
		}
		this.messagePasser.function = null;
	}

	public void handleVote(Message vote) throws UnknownHostException, IOException, InterruptedException{
		messagePasser.clockServiceInit();
		handleTimeStampedMessage(vote);
		System.out.println("BEING VOTED FOR MUTEX by " + vote.source);
		System.out.println("voteCount: " + (this.voteSet.size()-1));
		if(!this.voteMap.containsKey(vote.source)){
			this.voteMap.put(vote.source, vote);
		}
		System.out.println("VOTE COLLECTED: " + this.voteMap.size());
		if(this.voteMap.size() == this.voteSet.size()-1){
			System.out.println("VOTE COLLECTION COMPLETE!");
			state = MutexState.HELD;
			System.out.println(state);
			this.voteMap.clear();
		}
	}

	public void handleRelease(Message release) throws UnknownHostException, IOException, InterruptedException{
		messagePasser.clockServiceInit();
		handleTimeStampedMessage(release);
		if(this.mutexRequestQueue.size() != 0){
			Message requestMessage = this.mutexRequestQueue.poll();
			Message reply = new Message(requestMessage.source, "mutex_vote", null);
			reply.source = this.messagePasser.local_name;
			reply.sequenceNumber = this.messagePasser.generateSeqNum();
			reply.duplicate = requestMessage.duplicate;
			reply.groupNo = requestMessage.groupNo;
			messagePasser.send(reply);
			this.voted = true;
		}
		else {
			this.voted = false;
		}
	}
	
	
	public void handleTimeStampedMessage(Message receivedMsg){
		if (((TimeStampedMessage) receivedMsg).getClockType() == ClockType.LOGICAL) {
			int maxTimeStamp = Math.max(((LogicalClock) this.messagePasser.clockService).internalLogicalClock.timeStamp, ((TimeStampedMessage) receivedMsg).getLogicalTimeStamps().timeStamp);
			((LogicalClock) this.messagePasser.clockService).internalLogicalClock.timeStamp = maxTimeStamp;
		}

		if (((TimeStampedMessage) receivedMsg).getClockType() == ClockType.VECTOR) {
			for (int i = 0; i < this.messagePasser.processCount; i++) {
				if (i != this.messagePasser.processNo.value) {
					int maxTimeStamp = Math.max(((VectorClock) this.messagePasser.clockService).internalVectorClock.timeStampMatrix[i], ((TimeStampedMessage) receivedMsg).getVectorTimeStamps().timeStampMatrix[i]);
					((VectorClock) this.messagePasser.clockService).internalVectorClock.timeStampMatrix[i] = maxTimeStamp;
				}
			}
		}
	}
}

class LockWatcher extends Thread{
	
	Mutex mutex;
	public LockWatcher(Mutex mutex){
		this.mutex = mutex;
	}
	
	public void run(){
		while(this.mutex.state != MutexState.HELD){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("GET THE LOCK!");
	}
}

enum MutexState {
	HELD, RELEASED, WANTED;
}