
public class Node {
	String ip;
	int port;
	public Node(String ip, int port){
		this.ip = ip;
		this.port = port;
	}
	public String toString(){
		return ip + "," + Integer.toString(port);
	}
}
