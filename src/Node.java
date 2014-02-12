import java.util.ArrayList;


public class Node {
	String ip;
	int port;
	ArrayList<String> memberOf;
	public Node(String ip, int port, ArrayList<String> memberOf){
		this.ip = ip;
		this.port = port;
		this.memberOf = memberOf;
	}
	public String toString(){
		return ip + "," + Integer.toString(port);
	}
}
