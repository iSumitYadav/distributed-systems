package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;


import android.net.Uri;
import android.util.Log;
import android.os.Bundle;
import android.view.Menu;
import android.view.View;
import android.os.Process;
import android.app.Activity;
import android.os.AsyncTask;
import android.widget.Button;
import android.graphics.Color;
import android.text.Spannable;
import android.widget.TextView;
import android.widget.EditText;
import android.content.Context;
import android.text.SpannableString;
import android.content.ContentValues;
import android.telephony.TelephonyManager;
import android.text.style.ForegroundColorSpan;
import android.text.method.ScrollingMovementMethod;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] PORTS = new String[]{"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    int seq_no = 0;
    static final int process_id = Process.myPid();
    String myPort;
    int accepted_seq_no = 0;
    int proposedSeqNoPid = 0;
    int failed_port = -1;

    PriorityQueue<messageStruct> serverq = new PriorityQueue<messageStruct>(10, new serverqcomp());

    Map<Integer, Double> cdict = new HashMap<Integer, Double>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final Button sendButton = (Button) findViewById(R.id.button4);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        sendButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                final EditText editText = (EditText) findViewById(R.id.editText1);

                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                TextView tv = (TextView) findViewById(R.id.textView1);
                tv.append(msg);

                /*
                 * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                 * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                 * the difference, please take a look at
                 * http://developer.android.com/reference/android/os/AsyncTask.html
                 */
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, e.toString());
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            messageStruct ack = new messageStruct();
            messageStruct finalMsg = new messageStruct();

            String port = "";

            for(int i=0; i<PORTS.length; i++) {
                try {
                    port = PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    socket.setSoTimeout(100);

                    String msgToSend = msgs[0];
                    Log.e(TAG, "msgs msgToSend: "+msgs[0] +" to " + port);
                    String myPort = msgs[1];

                    messageStruct msgStruct = new messageStruct(
                            msgToSend,
                            Integer.parseInt(myPort)
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    if (!cdict.containsKey(msgStruct.r)) {
                        cdict.put(msgStruct.r, -1.1);
                    }

                    out.writeObject(msgStruct);
                    out.flush();




                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ack = (messageStruct) in.readObject();

                    Double psn_pid = Double.parseDouble(Integer.toString(ack.proposedSeqNo) + "." + Integer.toString(ack.pid));
                    if (cdict.get(ack.r) < psn_pid) {
                        cdict.remove(ack.r);
                        cdict.put(ack.r, psn_pid);
                        finalMsg = ack;
                    }

                    in.close();
                    socket.close();
                } catch (Exception e){
                    Log.e(TAG, "Client 1 ExceptionFinal: " + e.toString());
                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
                    continue;
                }
            }



            for (int i = 0; i < PORTS.length; i++) {
                try {
                    port = PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    socket.setSoTimeout(100);

                    finalMsg.deliverable = 1;

                    Log.e(TAG, "msgs deliverable: "+finalMsg.msg +" to " + port);

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(finalMsg);
                    out.flush();
                    socket.close();
                } catch (Exception e){
                    Log.e(TAG, "Client 2 ExceptionFinal: " + e.toString());
                    Log.e(TAG, "ClientPort 2 ExceptionFinal: " + port);
                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
                    continue;
                }
            }

            return null;
        }
    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Socket clientSocket = null;
            messageStruct msgPlusPortObject = new messageStruct();

            while(true){
                try {
                    clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(100);


                    ObjectInputStream in = null;
                    in = new ObjectInputStream(clientSocket.getInputStream());


                    msgPlusPortObject = (messageStruct) in.readObject();

                    if (msgPlusPortObject.deliverable == 1) {
                        serverq.add(msgPlusPortObject);
                    }


                    while(!serverq.isEmpty() && serverq.peek().deliverable == 1 && accepted_seq_no + 1 == serverq.peek().proposedSeqNo){
                        messageStruct m = serverq.poll();

                        if(failed_port != -1 || failed_port != m.port) {

                            Log.e(TAG, "Msg: " + m.msg + " : " + Double.parseDouble(Integer.toString(m.proposedSeqNo) + "." + Integer.toString(m.pid)));
                            String msgReceived = m.msg;
                            String showMsgReceived = "\t" + msgReceived;
                            Integer port = m.port;

                            Integer msgReceivedfrompid = m.pid;
                            String[] forPublishProgress = new String[]{showMsgReceived, Integer.toString(port), Integer.toString(msgReceivedfrompid), Integer.toString(m.proposedSeqNo)};


                            publishProgress(forPublishProgress);


                            ContentValues values = new ContentValues();
                            values.put(GroupMessengerProvider.COLUMN_NAME_KEY, Integer.toString(seq_no));
                            values.put(GroupMessengerProvider.COLUMN_NAME_VALUE, m.msg);
                            Uri uri = getContentResolver().insert(GroupMessengerProvider.CONTENT_URI, values);

                            accepted_seq_no = m.proposedSeqNo;
                            seq_no++;
                        }
                    }

                    if (msgPlusPortObject.deliverable != 1) {
                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());

                        msgPlusPortObject.proposedSeqNo = Math.max(accepted_seq_no, proposedSeqNoPid) + 1;
                        msgPlusPortObject.pid = process_id;

                        proposedSeqNoPid = msgPlusPortObject.proposedSeqNo;
                        out.writeObject(msgPlusPortObject);
                        out.flush();
                        out.close();
                    }

                    clientSocket.close();
                } catch (Exception e){
                    Log.e(TAG, "Server ExceptionFinal: " + e.toString());
                    e.printStackTrace();
                    continue;
                }
            }
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

            int fgColor = Color.YELLOW;
            String showMsgReceived = strings[0];
            Integer port = Integer.parseInt(strings[1]);

            switch (port){
                case 11108:
                    fgColor = Color.RED;
                    break;
                case 11112:
                    fgColor = Color.GREEN;
                    break;
                case 11116:
                    fgColor = Color.BLUE;
                    break;
                case 11120:
                    fgColor = Color.LTGRAY;
                    break;
                case 11124:
                    fgColor = Color.BLACK;
                    break;
            }

            Spannable colorMsg = new SpannableString(showMsgReceived);
            colorMsg.setSpan(new ForegroundColorSpan(fgColor), 0, showMsgReceived.length(), Spannable.SPAN_EXCLUSIVE_EXCLUSIVE);

            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(colorMsg);

            return;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

}


class messageStruct implements Serializable {
    String msg;
    Integer deliverable, proposedSeqNo, port, pid, r;

    public messageStruct(){
        deliverable = 0;
        proposedSeqNo = -1;


        msg = "";
        pid = -1;
        port = -1;

        Random rand = new Random();
        r = rand.nextInt(100000);
    }

    public messageStruct(String message, Integer avdport){
        deliverable = 0;
        proposedSeqNo = -1;
        pid = -1;


        msg = message;
        port = avdport;

        Random rand = new Random();
        r = rand.nextInt(100000);
    }
}

class serverqcomp implements Comparator<messageStruct> {

    public int compare(messageStruct msg1, messageStruct msg2) {
        if(msg1.proposedSeqNo == msg2.proposedSeqNo){
            if(msg1.pid < msg2.pid){
                return -1;
            }else if(msg1.pid > msg2.pid){
                return 1;
            }
            return 0;
        }else if(msg1.proposedSeqNo < msg2.proposedSeqNo){
            return -1;
        }else{
            return 1;
        }
    }
}