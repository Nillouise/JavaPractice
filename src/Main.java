import java.net.* ;
import java.io.* ;

class ActionSocket extends Thread{
    private Socket socket = null ;
    public ActionSocket(Socket s){
        this.socket = s ;
    }
    public void run(){
        try{
            this.action() ;
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void action() throws Exception {
        if (this.socket == null){
            return ;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        System.out.println(socket.getPort());
        System.out.println(socket.getInetAddress());

        for(String temp = br.readLine() ; temp!=null;temp = br.readLine() ){
            System.out.println(temp);
        }
        br.close();
    }
}


class linkToWeb
{
    Socket mSocket;
    int mWebPort;

    linkToWeb(int port)
    {
        mWebPort = port;
    }

    void link()  {
        OutputStream outputStream;
        InputStreamReader inputStream;
        FileInputStream filestream;
        try {
            filestream = new FileInputStream("C:\\code\\request.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            mSocket = new Socket(InetAddress.getLocalHost() ,mWebPort);
            mSocket.setSoTimeout(10000);
            outputStream =  mSocket.getOutputStream();
            byte b[] = new byte[1024];
            int n;
            while ((n=filestream.read(b))!=-1)
            {
                outputStream.write(b,0,n);
            }
            filestream.close();
            inputStream = new InputStreamReader(mSocket.getInputStream(),"utf-8");
            char buff[] = new char[1024];
            while (!mSocket.isClosed())
            {
                try {
                    if ((n=inputStream.read(buff))!=-1)
                        System.out.print(new String(buff,0,n));
                }catch (Exception e)
                {
                    e.printStackTrace();
                }

            }
            System.out.println("socket is closed");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    
    

}



public class Main{
    public static void main(String args[])throws Exception{

        linkToWeb l = new linkToWeb(26235);
        l.link();
    }
}