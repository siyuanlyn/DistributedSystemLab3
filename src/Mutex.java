import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;


public class Mutex {
	LinkedList<Message> mutexRequestQueue = new LinkedList<>();
	LinkedList<Message> receivedVote = new LinkedList<>();
	MessagePasser messagePasser;
	static MutexState state = MutexState.RELEASED;
	boolean voted = false;
	int voteCount = 0;
	HashMap<String, Message> voteMap = new HashMap<>();
	public Mutex(MessagePasser messagePasser){
		this.messagePasser = messagePasser;
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			int groupNo = Integer.parseInt(group.substring(5));
			for(int grpNo : messagePasser.multicast.groupMap.keySet()){
				if(grpNo == groupNo){
					voteCount += messagePasser.multicast.groupMap.get(grpNo).size();
					voteCount--;
				}
			}
		}
	}

	public void request() throws IOException, InterruptedException{
		state = MutexState.WANTED;
		System.out.println("MUTEX LOCK WANTED!");
		int seqNo = messagePasser.generateSeqNum();
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			Message request = new Message(group, "mutex_request", null);
			request.source = this.messagePasser.local_name;
			request.sequenceNumber = seqNo;
			request.groupNo = Integer.parseInt(group.substring(5));

			messagePasser.multicast.send(request);
		}
		//		synchronized (state) {
		while(state != MutexState.HELD){
//			System.out.println(state);
			Thread.sleep(1000);
		}
		System.out.println("GET THE LOCK!");
		//		}

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
		state = MutexState.RELEASED;
		int seqNo = messagePasser.generateSeqNum();
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			Message release = new Message(group, "mutex_release", null);
			release.source = this.messagePasser.local_name;
			release.sequenceNumber = seqNo;
			release.groupNo = Integer.parseInt(group.substring(5));

			messagePasser.multicast.send(release);
			handleRelease(release);
		}
	}

	public void handleVote(Message vote){
		System.out.println("BEING VOTED FOR MUTEX by " + vote.source);
		System.out.println("voteCount: " + this.voteCount);
		if(!this.voteMap.containsKey(vote.source)){
			this.voteMap.put(vote.source, vote);
		}
		if(this.voteMap.size() == this.voteCount){
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