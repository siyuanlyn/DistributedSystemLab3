import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;


public class Mutex {
	LinkedList<Message> mutexRequestQueue = new LinkedList<>();
	LinkedList<Message> receivedVote = new LinkedList<>();
	MessagePasser messagePasser;
	static MutexState state = MutexState.RELEASED;
	boolean voted = false;
	//	int voteCount = 0;
	HashMap<String, Message> voteMap = new HashMap<>();
	HashSet<String> voteSet = new HashSet<>();
	public Mutex(MessagePasser messagePasser){
		this.messagePasser = messagePasser;
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			int groupNo = Integer.parseInt(group.substring(5));
			for(int grpNo : messagePasser.multicast.groupMap.keySet()){
				if(grpNo == groupNo){
					//					voteCount += messagePasser.multicast.groupMap.get(grpNo).size();
					//					voteCount--;
					for(String groupMember : messagePasser.multicast.groupMap.get(grpNo)){
						voteSet.add(groupMember);
					}
				}
			}
		}
	}

	public void request() throws IOException, InterruptedException{
		this.messagePasser.function = Function.REQUEST_MUTEX;
		state = MutexState.WANTED;
		System.out.println("MUTEX LOCK WANTED!");
		int seqNo = messagePasser.generateSeqNum();

		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			Message request = new Message(group, "mutex_request", null);
			request.source = this.messagePasser.local_name;
			request.sequenceNumber = seqNo;
			request.groupNo = Integer.parseInt(group.substring(5));

			messagePasser.multicast.send(request);
			((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
		}
		((VectorClock)this.messagePasser.clockService).ticks();
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
		//		synchronized (state) {
		while(state != MutexState.HELD){
			//			System.out.println(state);
			Thread.sleep(1000);
		}
		System.out.println("GET THE LOCK!");
		//		}
		this.messagePasser.function = null;
	}

	public void handleRequest(Message request) throws UnknownHostException, IOException, InterruptedException{
		if(state == MutexState.HELD || this.voted){
			this.mutexRequestQueue.offer(request);
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
		this.messagePasser.function = Function.RELEASE_MUTEX;
		state = MutexState.RELEASED;
		int seqNo = messagePasser.generateSeqNum();
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			Message release = new Message(group, "mutex_release", null);
			release.source = this.messagePasser.local_name;
			release.sequenceNumber = seqNo;
			release.groupNo = Integer.parseInt(group.substring(5));

			messagePasser.multicast.send(release);
			((VectorClock)messagePasser.clockService).internalVectorClock.timeStampMatrix[messagePasser.processNo.value]--;
			handleRelease(release);
		}
		((VectorClock)this.messagePasser.clockService).ticks();
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

	public void handleVote(Message vote){
		System.out.println("BEING VOTED FOR MUTEX by " + vote.source);
		System.out.println("voteCount: " + (this.voteSet.size()-1));
		if(!this.voteMap.containsKey(vote.source)){
			this.voteMap.put(vote.source, vote);
		}
		System.out.println("VOTE COLLECTED: " + this.voteMap.size());
		if(this.voteMap.size() == this.voteSet.size()-1){
			System.out.println("VOTE COLLECTION COMPLETE!");
			//			synchronized (state) {
			state = MutexState.HELD;
			System.out.println(state);
			//			}
			this.voteMap.clear();
		}
	}

	public void handleRelease(Message release) throws UnknownHostException, IOException, InterruptedException{
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
}

enum MutexState {
	HELD, RELEASED, WANTED;
}