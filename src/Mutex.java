import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;


public class Mutex {
	LinkedList<Message> mutexRequestQueue = new LinkedList<>();
	LinkedList<Message> receivedVote = new LinkedList<>();
	MessagePasser messagePasser;
	MutexState state = MutexState.RELEASED;
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
				}
			}
		}
	}
	
	public void request() throws IOException, InterruptedException{
		this.state = MutexState.WANTED;
		int seqNo = messagePasser.generateSeqNum();
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			Message request = new Message(group, "mutex_request", null);
			request.source = this.messagePasser.local_name;
			request.sequenceNumber = seqNo;
			request.groupNo = Integer.parseInt(group.substring(5));
			
			messagePasser.multicast.send(request);
		}
		synchronized (this.state) {
			while(this.state != MutexState.HELD){
				
			}
			System.out.println("GET THE LOCK!");
		}
		
	}
	
	public void handleRequest(Message request) throws UnknownHostException, IOException, InterruptedException{
		if(this.state == MutexState.HELD || this.voted){
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
		this.state = MutexState.RELEASED;
		int seqNo = messagePasser.generateSeqNum();
		for(String group : this.messagePasser.nodeMap.get(this.messagePasser.local_name).memberOf){
			Message request = new Message(group, "mutex_release", null);
			request.source = this.messagePasser.local_name;
			request.sequenceNumber = seqNo;
			request.groupNo = Integer.parseInt(group.substring(5));
			
			messagePasser.multicast.send(request);
		}
	}
	
	public void handleVote(Message vote){
		if(!this.voteMap.containsKey(vote.source)){
			this.voteMap.put(vote.source, vote);
		}
		if(this.voteMap.size() == this.voteCount){
			synchronized (this.state) {
				this.state = MutexState.HELD;
			}
		}
	}
	
	public void handleRelease(Message release) throws UnknownHostException, IOException, InterruptedException{
		if(this.mutexRequestQueue.size() != 0){
			Message requestMessage = this.mutexRequestQueue.poll();
			Message reply = new Message(requestMessage.source, "mutex_vote", null);
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