package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Formatter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.telephony.TelephonyManager;

import android.content.Context;
import android.content.ContentValues;
import android.content.ContentProvider;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;


public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");

    private SQLiteDatabase db;
    public static final String TABLE_NAME = "messages";
    public static final String COLUMN_NAME_KEY = "key";
    public static final String COLUMN_NAME_VALUE = "value";

    static final int SERVER_PORT = 10000;
    static final String CONNECT_PORT = "11108";

    String myPort = null;
    String portStr = null;
    String myPortHash = null;

    String successor = null;
    String predecessor = null;

//    TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
//    String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//    String myPort = String.valueOf((Integer.parseInt(portStr) * 2));


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = (String) values.get("key");
        try {
            key = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "CP insert " + e.toString());
        }

//        String node1 = genHash("5554");
//        String node2 = genHash("5556");
//        String node3 = genHash("5558");
//        String node4 = genHash("5560");
//        String node5 = genHash("5562");
//        Relation is as follows: 5 < 2 < 1 < 3 < 4

//        String node1 = genHash("11108");
//        String node2 = genHash("11112");
//        String node3 = genHash("11116");
//        String node4 = genHash("11120");
//        String node5 = genHash("11124");
//        Relation is as follows: 4 < 2 < 3 < 1 < 5

        if (successor == null) {
            db.insert(TABLE_NAME, null, values);
        } else if (myPortHash.compareTo(key) >= 0 && key.compareTo(predecessor) > 0) {
            db.insert(TABLE_NAME, null, values);
        } else {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, (String) values.get("key"), (String) values.get("value"));
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            SimpleDHTDBHelper DBHelper = new SimpleDHTDBHelper(getContext());

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));

            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, e.toString());
                Log.e(TAG, "Can't create a ServerSocket");
                return false;
            }

            try {
                myPortHash = genHash(portStr);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "CP onCreate myPortHash " + e.toString());
            }

            if (myPort.equals(CONNECT_PORT)) {
                predecessor = null;
            } else {
                // connect to 5554
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", CONNECT_PORT);
            }


            db = DBHelper.getWritableDatabase();
            if (db != null) {
                return true;
            }

        } catch (Exception e) {
            Log.e(TAG, "CP onCreate Exception " + e.toString());
        } finally {
            return false;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Cursor cursor;

        Log.d(TAG, selection);
        if (selection.equals("*") || selection.equals("@")) {
            cursor = db.query(
                TABLE_NAME,
                projection,
                null,
                null,
                null,
                null,
                sortOrder,
                null
            );
            Log.d("qKEY", selection);

            return cursor;
        } else {
            String hashKey = null;
            try {
                hashKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "CP query " + e.toString());
            }

            if (predecessor == null || (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessor) > 0)) {
                String[] selectionArgss = new String[]{selection};

                selection = COLUMN_NAME_KEY + "=?";

                cursor = db.query(
                    TABLE_NAME,
                    projection,
                    selection,
                    selectionArgss,
                    null,
                    null,
                    sortOrder,
                    "1"
                );

                Log.d("qKEY", selectionArgss[0].toString());
                try {
                    if (cursor == null) {
                        Log.d("cursor is Null", "-");
                    } else {
                        Log.d("cursor is NOT Null", "-");
                    }
//                    Log.d("cursor is Null", DatabaseUtils.dumpCursorToString(cursor));
                    cursor.moveToFirst();
                    Log.d("qKEY cursor", cursor.getString(0));
                    Log.d("qVALUE cursor", cursor.getString(1));
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                    e.printStackTrace();
                }

                return cursor;
            } else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "search", successor, selection);
            }
        }

        return null;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            messageStruct ack = new messageStruct();
            messageStruct finalMsg = new messageStruct();

            String port = "";
            String msgType = msgs[0];

            if (msgType.equals("connect")) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(CONNECT_PORT));
                    socket.setSoTimeout(100);

//                    String msgToSend = msgs[0];
//                    Log.e(TAG, "msgs msgToSend: "+msgs[0] +" to " + port);
                    String myPort = msgs[1];

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        myPort
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

//                    if (!cdict.containsKey(msgStruct.r)) {
//                        cdict.put(msgStruct.r, -1.1);
//                    }

                    out.writeObject(msgStruct);
                    out.flush();


                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ack = (messageStruct) in.readObject();

                    successor = ack.successor;
                    predecessor = ack.predecessor;

//                    Double psn_pid = Double.parseDouble(Integer.toString(ack.proposedSeqNo) + "." + Integer.toString(ack.pid));
//                    if (cdict.get(ack.r) < psn_pid) {
//                        cdict.remove(ack.r);
//                        cdict.put(ack.r, psn_pid);
//                        finalMsg = ack;
//                    }

                    in.close();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Client 1 ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
//                    continue;
                }
            } else if (msgType.equals("insert")) {
                String nxtSuccessor = msgs[1];
                String key = msgs[2];
                String value = msgs[3];

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
                    socket.setSoTimeout(100);

//                    String msgToSend = msgs[0];
//                    Log.e(TAG, "msgs msgToSend: "+msgs[0] +" to " + port);
                    String myPort = msgs[1];

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        key,
                        value
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

//                    if (!cdict.containsKey(msgStruct.r)) {
//                        cdict.put(msgStruct.r, -1.1);
//                    }

                    out.writeObject(msgStruct);
                    out.flush();


//                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
//                    ack = (messageStruct) in.readObject();
//
////                    Double psn_pid = Double.parseDouble(Integer.toString(ack.proposedSeqNo) + "." + Integer.toString(ack.pid));
////                    if (cdict.get(ack.r) < psn_pid) {
////                        cdict.remove(ack.r);
////                        cdict.put(ack.r, psn_pid);
////                        finalMsg = ack;
////                    }
//
//                    in.close();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Client 1 ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
//                    continue;
                }
            } else if (msgType.equals("search")) {
                String nxtSuccessor = msgs[1];
                String key = msgs[2];

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
                    socket.setSoTimeout(100);

//                    String msgToSend = msgs[0];
//                    Log.e(TAG, "msgs msgToSend: "+msgs[0] +" to " + port);
                    String myPort = msgs[1];

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        key
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

//                    if (!cdict.containsKey(msgStruct.r)) {
//                        cdict.put(msgStruct.r, -1.1);
//                    }

                    out.writeObject(msgStruct);
                    out.flush();


                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ack = (messageStruct) in.readObject();

//                    Double psn_pid = Double.parseDouble(Integer.toString(ack.proposedSeqNo) + "." + Integer.toString(ack.pid));
//                    if (cdict.get(ack.r) < psn_pid) {
//                        cdict.remove(ack.r);
//                        cdict.put(ack.r, psn_pid);
//                        finalMsg = ack;
//                    }

                    in.close();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Client 1 ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
//                    continue;
                }
            }



//            for (int i = 0; i < PORTS.length; i++) {
//                try {
//                    port = PORTS[i];
//                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
//                    socket.setSoTimeout(100);
//
//                    finalMsg.deliverable = 1;
//
//                    Log.e(TAG, "msgs deliverable: "+finalMsg.msg +" to " + port);
//
//                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//                    out.writeObject(finalMsg);
//                    out.flush();
//                    socket.close();
//                } catch (Exception e){
//                    Log.e(TAG, "Client 2 ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 2 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
//                    e.printStackTrace();
//                    continue;
//                }
//            }

            return null;
        }
    }


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

                    if (msgPlusPortObject.msg.equals("connect")) {
                        String newNodeHash = null;
                        String port = msgPlusPortObject.port.toString();

                        try{
                            Integer emulatorId = Integer.parseInt(port)/2;
                            newNodeHash = genHash(emulatorId.toString());
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "newNode join " + e.toString());
                        }

                        if (myPort.equals(CONNECT_PORT)) {
                            if (successor == null) {
                                predecessor = port;
                                successor = port;
                            } else if (successor.compareTo(newNodeHash) >= 0 && myPortHash.compareTo(newNodeHash) < 0) {
                                ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());

                                msgPlusPortObject.predecessor = myPort;
                                msgPlusPortObject.successor = successor;

                                successor = port;

                                out.writeObject(msgPlusPortObject);
                                out.flush();
                                out.close();
                            } else {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", successor);
                            }
                        } else {
//                             Can't happen coz all join requests are handled by 5554 only
                        }
                    } else if (msgPlusPortObject.msg.equals("insert")) {
                        ContentValues values = new ContentValues();
                        values.put(SimpleDhtProvider.COLUMN_NAME_KEY, msgPlusPortObject.key);
                        values.put(SimpleDhtProvider.COLUMN_NAME_VALUE, msgPlusPortObject.value);
//                        Uri uri = getContentResolver().insert(SimpleDhtProvider.CONTENT_URI, values);
                        insert(CONTENT_URI, values);
                    } else if (msgPlusPortObject.msg.equals("search")) {
                        query(CONTENT_URI, null, msgPlusPortObject.key, null, null);
                    }


//                    while(!serverq.isEmpty() && serverq.peek().deliverable == 1 && accepted_seq_no + 1 == serverq.peek().proposedSeqNo){
//                        messageStruct m = serverq.poll();
//
//                        if(failed_port != -1 || failed_port != m.port) {
//
//                            Log.e(TAG, "Msg: " + m.msg + " : " + Double.parseDouble(Integer.toString(m.proposedSeqNo) + "." + Integer.toString(m.pid)));
//                            String msgReceived = m.msg;
//                            String showMsgReceived = "\t" + msgReceived;
//                            Integer port = m.port;
//
//                            Integer msgReceivedfrompid = m.pid;
//                            String[] forPublishProgress = new String[]{showMsgReceived, Integer.toString(port), Integer.toString(msgReceivedfrompid), Integer.toString(m.proposedSeqNo)};
//
//
//                            publishProgress(forPublishProgress);
//
//
//                            ContentValues values = new ContentValues();
//                            values.put(GroupMessengerProvider.COLUMN_NAME_KEY, Integer.toString(seq_no));
//                            values.put(GroupMessengerProvider.COLUMN_NAME_VALUE, m.msg);
//                            Uri uri = getContentResolver().insert(GroupMessengerProvider.CONTENT_URI, values);
//
//                            accepted_seq_no = m.proposedSeqNo;
//                            seq_no++;
//                        }
//                    }
//
//                    if (msgPlusPortObject.deliverable != 1) {
//                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
//
//                        msgPlusPortObject.proposedSeqNo = Math.max(accepted_seq_no, proposedSeqNoPid) + 1;
//                        msgPlusPortObject.pid = process_id;
//
//                        proposedSeqNoPid = msgPlusPortObject.proposedSeqNo;
//                        out.writeObject(msgPlusPortObject);
//                        out.flush();
//                        out.close();
//                    }

                    clientSocket.close();
                } catch (Exception e){
                    Log.e(TAG, "Server ExceptionFinal: " + e.toString());
                    e.printStackTrace();
                    continue;
                }
            }
        }

//        protected void onProgressUpdate(String... strings) {
//            /*
//             * The following code displays what is received in doInBackground().
//             */
//
//            int fgColor = Color.YELLOW;
//            String showMsgReceived = strings[0];
//            Integer port = Integer.parseInt(strings[1]);
//
//            switch (port){
//                case 11108:
//                    fgColor = Color.RED;
//                    break;
//                case 11112:
//                    fgColor = Color.GREEN;
//                    break;
//                case 11116:
//                    fgColor = Color.BLUE;
//                    break;
//                case 11120:
//                    fgColor = Color.LTGRAY;
//                    break;
//                case 11124:
//                    fgColor = Color.BLACK;
//                    break;
//            }
//
//            Spannable colorMsg = new SpannableString(showMsgReceived);
//            colorMsg.setSpan(new ForegroundColorSpan(fgColor), 0, showMsgReceived.length(), Spannable.SPAN_EXCLUSIVE_EXCLUSIVE);
//
//            TextView tv = (TextView) findViewById(R.id.textView1);
//            tv.append(colorMsg);
//
//            return;
//        }
    }

//  ================================================================================

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    public class SimpleDHTDBHelper extends SQLiteOpenHelper{
        public static final int DATABASE_VERSION = 1;
        private final String SQL_CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (" + COLUMN_NAME_KEY + " TEXT," + COLUMN_NAME_VALUE + " TEXT)";


        public SimpleDHTDBHelper(Context context) {
//            Made in-memory database by not specifying 2nd arg name with DBName.
            super(context, null, null, DATABASE_VERSION);
        }

        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_CREATE_TABLE);
        }

        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }

        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onUpgrade(db, oldVersion, newVersion);
        }
    }

    class messageStruct implements Serializable {
        String msg, port, key, value, successor, predecessor;

        public messageStruct(){
            msg = "";
            port = null;
            successor = null;
            predecessor = null;
        }

        public messageStruct(String message, String avdport){
            msg = message;
            port = avdport;
            successor = null;
            predecessor = null;
        }

        public messageStruct(String message, String _key, String _value){
            msg = message;
            key = _key;
            value = _value;
        }
    }
}
