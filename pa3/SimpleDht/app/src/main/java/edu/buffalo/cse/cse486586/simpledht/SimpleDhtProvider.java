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


import android.annotation.SuppressLint;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.AsyncTask;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.telephony.TelephonyManager;

import android.content.Context;
import android.content.ContentValues;
import android.content.ContentProvider;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.widget.TextView;


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
    String successorHash = null;
    String predecessorHash = null;

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

//        String originatorPort = myPort;
//        if (values.containsKey("originator")) {
//            originatorPort = (String) values.get("originator");
//            values.remove("originator");
//        }

        String originalKey = (String) values.get("key");
        String hashedKey = null;
        try {
            hashedKey = genHash(originalKey);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "CP insert " + e.toString());
        }

        try {
            if (successor != null) {
                successorHash = genHash(String.valueOf((Integer.parseInt(successor) / 2)));
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "successorHash " + e.toString());
        }

        try {
            if (predecessor != null) {
                predecessorHash = genHash(String.valueOf((Integer.parseInt(predecessor) / 2)));
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "predecessorHash " + e.toString());
        }

        Log.d(TAG,
                "CP insert start myport" + myPort + " key " + originalKey +
                        " hashedKey " + hashedKey);
//        Log.d(TAG,
//                "CP insert myport " + myPort + " originatorPort " + originatorPort + " .equals " + Boolean.toString(myPort.equals(originatorPort)));
        if (successor == null) {
            Log.d(TAG, "CP insert succ NULL myport" + myPort + " key " + originalKey);
            db.insert(TABLE_NAME, null, values);
        } else if (successor == predecessor) {
            if (hashedKey.compareTo(myPortHash) <= 0 && hashedKey.compareTo(successorHash) < 0) {
                db.insert(TABLE_NAME, null, values);
            } else if (hashedKey.compareTo(myPortHash) > 0 && hashedKey.compareTo(successorHash) < 0) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"));
            } else if (hashedKey.compareTo(myPortHash) > 0 && hashedKey.compareTo(successorHash) > 0) {
                db.insert(TABLE_NAME, null, values);
            }
        } else if (myPortHash.compareTo(hashedKey) >=
            0 && hashedKey.compareTo(predecessorHash) > 0) {
            Log.d(TAG, "CP insert NODE FOUND myport" + myPort + " key " + originalKey);
            db.insert(TABLE_NAME, null, values);
//        } else if (myPort.equals(originatorPort) && (myPortHash.compareTo(hashedKey) < 0 && predecessorHash.compareTo(hashedKey) < 0) || (myPortHash.compareTo(hashedKey) > 0 && predecessorHash.compareTo(hashedKey) > 0)) {
        } else if (myPortHash.compareTo(hashedKey) < 0 && predecessorHash.compareTo(hashedKey) < 0 && successorHash.compareTo(hashedKey) < 0 && myPortHash.compareTo(predecessorHash) < 0) {
            Log.d(TAG, "CP insert bw last node before 2^m -1 myport" + myPort + " key " + originalKey);
// || (myPortHash.compareTo(hashedKey) < 0 && predecessorHash.compareTo(hashedKey) > 0)
            db.insert(TABLE_NAME, null, values);
//            if (myPort.equals(originatorPort)) {
//                db.insert(TABLE_NAME, null, values);
//            } else {
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"), originatorPort);
//            }


//        } else if (myPortHash.compareTo(hashedKey) > 0 && predecessorHash.compareTo(hashedKey) > 0){
//            Log.d(TAG, "CP insert bw last node after 0 but before first node" + " myport" + myPort + " key " + originalKey);
//
//            if (originatorPort != null && myPort.equals(originatorPort)) {
//                db.insert(TABLE_NAME, null, values);
//            } else {
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"), originatorPort);
//            }
        } else if (myPortHash.compareTo(hashedKey) > 0 && predecessorHash.compareTo(hashedKey) > 0 && successorHash.compareTo(hashedKey) > 0 && myPortHash.compareTo(predecessorHash) < 0) {
            db.insert(TABLE_NAME, null, values);
        } else {
            Log.d(TAG,
                    "CP insert referred to succ " + successor + " myport" + myPort + " key " + originalKey);
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
//                    "insert", successor, originalKey, (String) values.get(
//                            "value"), null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"));
        }
        Log.d(TAG, "CP insert done myport" + myPort + " key " + originalKey);

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            Log.d(TAG, "cp onCreate Start");
            SimpleDHTDBHelper DBHelper = new SimpleDHTDBHelper(getContext());

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));

            try {
                Log.d(TAG, "cp onCreate start server");
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                Log.d(TAG, "cp onCreate server started");
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


            if (!myPort.equals(CONNECT_PORT)) {
                // connect to 5554
                Log.d(TAG, "cp onCreate CONNECT_PORT from "+myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        "connect", CONNECT_PORT, myPort);
                Log.d(TAG, "cp onCreate CONNECT_PORT done from "+myPort);
            }

//            final TextView tv = (TextView) findViewById(R.id.textView1);
//            tv.setMovementMethod(new ScrollingMovementMethod());
//            findViewById(R.id.button3).setOnClickListener(new OnTestClickListener(tv, getContentResolver()));


            db = DBHelper.getWritableDatabase();
            Log.d(TAG, "cp onCreate stop");
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

        Log.d(TAG,
                "QUERY GLOBAL myPort: " + myPort + "," + " succ: "+ successor +
                        ", " + "pred: "+predecessor);

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

            try {
                if (predecessor != null) {
                    predecessorHash = genHash(String.valueOf((Integer.parseInt(predecessor) / 2)));
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "predecessorHash " + e.toString());
            }

            if (predecessor == null || (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessorHash) > 0)) {
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
            String nxtSuccessor = msgs[1];
            Log.d(TAG, "lololololog: "+myPort+" " +nxtSuccessor);
//            if (msgType.equals("connect") && !myPort.equals(CONNECT_PORT)) {
            if (msgType.equals("connect")) {
                try {
                    String originatorPort = msgs[2];
                    Log.d(TAG,
                            "ClientTask connect started from " + msgs[2] + " " +
                                    "for " + nxtSuccessor);
                    Socket socket =
                            new Socket(InetAddress.getByAddress(new byte[]{10
                                    , 0, 2, 2}), Integer.parseInt(nxtSuccessor));
//                    socket.setSoTimeout(100);

//                    String msgToSend = msgs[0];
//                    Log.e(TAG, "msgs msgToSend: "+msgs[0] +" to " + port);
//                    String myPort = nxtSuccessor;

                    messageStruct msgStruct = new messageStruct(
                            msgType,
                            msgs[2]
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

//                    if (!cdict.containsKey(msgStruct.r)) {
//                        cdict.put(msgStruct.r, -1.1);
//                    }

                    out.writeObject(msgStruct);
                    out.flush();

                    Log.d(TAG,
                            "ClientTask connect msg out from " + msgs[2] + " " +
                                    "for " + nxtSuccessor);

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ack = (messageStruct) in.readObject();

//                    if (ack.successor == null || ack.predecessor == null) {
//
//                    }

                    if (ack.successor != null && ack.successor.length() > 0) {
                        successor = ack.successor;
                    }
                    if (ack.predecessor != null && ack.predecessor.length() > 0) {
                        predecessor = ack.predecessor;
                    }

//                    final String myPortFinal = myPort;
                    Log.d(TAG, "ClientTask connect ack in from " + msgs[2] + " for " + nxtSuccessor);
                    Log.d(TAG, "ClientTask connect myPort: " + msgs[2] + ", " + "succ: " + successor + ", pred: " + predecessor);
//                    tv.append("myPort: " + myPort + ", succ: "+ successor + ", pred: "+predecessor);
//                    tv.post(new Runnable() {
//                        public void run() {
//                            tv.append("myPort: " + myPortFinal + ", succ: "+ successor + ", pred: "+predecessor);
//                        }
//                    });

//                    Double psn_pid = Double.parseDouble(Integer.toString(ack.proposedSeqNo) + "." + Integer.toString(ack.pid));
//                    if (cdict.get(ack.r) < psn_pid) {
//                        cdict.remove(ack.r);
//                        cdict.put(ack.r, psn_pid);
//                        finalMsg = ack;
//                    }

                    in.close();
                    socket.close();
                    Log.d(TAG, "ClientTask connect done from " + msgs[2] + " " +
                            "for " + nxtSuccessor);
                } catch (Exception e) {
                    Log.e(TAG, "Client connect ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
//                    continue;
                }
//            } else if (msgType.equals("connectTo")) {
//                try {
//                    String originatorPort = msgs[2];
//                    Log.d(TAG,
//                            "ClientTask connect started from " + msgs[2] + " " +
//                                    "for " + nxtSuccessor);
//                    Socket socket =
//                            new Socket(InetAddress.getByAddress(new byte[]{10
//                                    , 0, 2, 2}), Integer.parseInt(nxtSuccessor));
//
//
//                    messageStruct msgStruct = new messageStruct(
//                            msgType,
//                            msgs[2]
//                    );
//
//                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//                    out.writeObject(msgStruct);
//                    out.flush();
//
////                    Log.d(TAG,
////                            "ClientTask connect msg out from " + msgs[2] + " " +
////                                    "for " + nxtSuccessor);
//
//
//
////                    final String myPortFinal = myPort;
////                    Log.d(TAG, "ClientTask connect ack in from " + msgs[2] + " for " + nxtSuccessor);
////                    Log.d(TAG, "ClientTask connect myPort: " + msgs[2] + ", " + "succ: " + successor + ", pred: " + predecessor);
//
////                    in.close();
//                    socket.close();
////                    Log.d(TAG, "ClientTask connect done from " + msgs[2] + " " +
////                            "for " + nxtSuccessor);
//                } catch (Exception e) {
//                    Log.e(TAG,
//                            "Client connectTo ExceptionFinal: " + e.toString());
////                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
////                    failed_port = Integer.parseInt(port);
//                    e.printStackTrace();
////                    continue;
//                }
            } else if (msgType.equals("adjustRing")) {
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", successor, "predecessor", port);
                try {
                    String adjustPort = msgs[1];
                    String adjustNode = msgs[2];
                    String adjustNodeToPort = msgs[3];
                    Log.d(TAG, "Adjust Ring called for " + adjustPort  + " to assign " + adjustNode + " to port " + adjustNodeToPort);
                    Socket socket =new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(adjustPort));

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        adjustNode,
                        adjustNodeToPort
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    out.writeObject(msgStruct);
                    out.flush();

//                    Log.d(TAG,
//                            "ClientTask connect msg out from " + msgs[2] + " " +
//                                    "for " + nxtSuccessor);

//                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
//                    ack = (messageStruct) in.readObject();

//                    Log.d(TAG, "AdjustRing ack in from " + msgs[2] + " for " + nxtSuccessor);
//                    Log.d(TAG,
//                            "Ring Adjusted myPort: " + myPort + ", " + "succ" +
//                                    ": " + successor + ", pred: " + predecessor);

//                    in.close();
                    socket.close();
//                    Log.d(TAG, "ClientTask connect done from " + msgs[2] + " " +
//                            "for " + nxtSuccessor);
                } catch (Exception e) {
                    Log.e(TAG,
                            "Adjust connect ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
//                    continue;
                }
            } else if (msgType.equals("insert")) {
                String key = msgs[2];
                String value = msgs[3];
//                String originatorPort = msgs[4];

                try {
                    Log.d(TAG, "ClientTask insert started from " + myPort + " for "+nxtSuccessor + " key " +key);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
//                    socket.setSoTimeout(100);

//                    String msgToSend = msgs[0];
//                    Log.e(TAG, "msgs msgToSend: "+msgs[0] +" to " + port);
//                    String myPort = msgs[1];

                    messageStruct msgStruct = new messageStruct(
                        msgType,
//                        originatorPort,
                            null,
                        key,
                        value
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

//                    if (!cdict.containsKey(msgStruct.r)) {
//                        cdict.put(msgStruct.r, -1.1);
//                    }

                    out.writeObject(msgStruct);
                    out.flush();

                    Log.d(TAG, "ClientTask insert msg out from " + myPort + " for "+nxtSuccessor + " key " +key);

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
                    Log.d(TAG, "ClientTask insert done from " + myPort + " for "+nxtSuccessor + " key " +key);
                } catch (Exception e) {
                    Log.e(TAG, "Client insert ExceptionFinal: " + e.toString());
//                    Log.e(TAG, "ClientPort 1 ExceptionFinal: " + port);
//                    failed_port = Integer.parseInt(port);
                    e.printStackTrace();
//                    continue;
                }
            } else if (msgType.equals("search")) {
                String key = msgs[2];

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
//                    socket.setSoTimeout(100);

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
                    Log.e(TAG, "Client search ExceptionFinal: " + e.toString());
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

//        @SuppressLint("WrongThread")
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            messageStruct msgPlusPortObject = new messageStruct();

            while(true){
                Log.d(TAG,
                        "GLOBAL myPort: " + myPort + "," +
                                " succ: "+ successor + ", pred: "+predecessor);
                try {
                    Socket clientSocket = null;
                    clientSocket = serverSocket.accept();
//                    clientSocket.setSoTimeout(100);

                    ObjectInputStream in = null;
                    in = new ObjectInputStream(clientSocket.getInputStream());

                    msgPlusPortObject = (messageStruct) in.readObject();

                    if (msgPlusPortObject.msg.equals("connect")) {
                        String newNodeHash = null;
//                        String port = msgPlusPortObject.port.toString();
                        String port = msgPlusPortObject.port;

                        Log.d(TAG,
                                "Connect Request to myPort: " + myPort + "," +
                                        " succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);

                        try{
                            Integer emulatorId = Integer.parseInt(port)/2;
                            newNodeHash = genHash(emulatorId.toString());
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "emulatorId hash " + e.toString());
                        }

                        try{
                            if (successor != null) {
                                successorHash = genHash(String.valueOf((Integer.parseInt(successor) / 2)));
                            }
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "successorHash " + e.toString());
                        }

                        try{
                            if (predecessor != null) {
                                predecessorHash = genHash(String.valueOf((Integer.parseInt(predecessor) / 2)));
                            }
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "predecessorHash " + e.toString());
                        }
                        Log.d(TAG,
                                "Connect Request in bw1 to myPort: " + myPort +
                                        "," +
                                        " succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);
                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                        Log.d(TAG,
                                "Connect Request in bw2 to myPort: " + myPort +
                                        "," +
                                        " succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);
//                        if (myPort.equals(CONNECT_PORT)) {
                        if (successor == null) {
                            predecessor = port;
                            successor = port;

                            Log.d(TAG, " Succ is Null myPort: " + myPort + ", succ: " + successor + ", pred: " + predecessor + " reqFromPort " + port);

                            msgPlusPortObject.successor = CONNECT_PORT;
                            msgPlusPortObject.predecessor = CONNECT_PORT;

//                            out.writeObject(msgPlusPortObject);
//                            out.flush();
//                            out.close();
                        } else if (successor == predecessor) {
                            if (newNodeHash.compareTo(successorHash) > 0) {
                                if (newNodeHash.compareTo(myPortHash) < 0) {
                                    String[] forPublishProgress = new String[]{
                                            "adjustRing",
                                            predecessor,
                                            "successor",
                                            port
                                    };
                                    publishProgress(forPublishProgress);

                                    forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "successor",
                                            myPort
                                    };
                                    publishProgress(forPublishProgress);

                                    forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "predecessor",
                                            predecessor
                                    };
                                    publishProgress(forPublishProgress);

                                    predecessor = port;
                                } else if (newNodeHash.compareTo(myPortHash) > 0) {
                                    if (myPortHash.compareTo(successorHash) > 0) {
                                        String[] forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "predecessor",
                                                myPort
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "successor",
                                                successor
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                successor,
                                                "predecessor",
                                                port
                                        };
                                        publishProgress(forPublishProgress);

                                        successor = port;
                                    } else {
                                        String[] forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "predecessor",
                                                successor
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "successor",
                                                myPort
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                successor,
                                                "successor",
                                                port
                                        };
                                        publishProgress(forPublishProgress);

                                        predecessor = port;
                                    }
                                }
                            } else if (newNodeHash.compareTo(successorHash) < 0) {
                                if (newNodeHash.compareTo(myPortHash) < 0) {
                                    if (myPortHash.compareTo(successorHash) > 0) {
                                        String[] forPublishProgress = new String[]{
                                                "adjustRing",
                                                successor,
                                                "predecessor",
                                                port
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "successor",
                                                predecessor
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "predecessor",
                                                myPort
                                        };
                                        publishProgress(forPublishProgress);

                                        successor = port;
                                    } else {
                                        String[] forPublishProgress = new String[]{
                                                "adjustRing",
                                                successor,
                                                "successor",
                                                port
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "predecessor",
                                                successor
                                        };
                                        publishProgress(forPublishProgress);

                                        forPublishProgress = new String[]{
                                                "adjustRing",
                                                port,
                                                "successor",
                                                myPort
                                        };
                                        publishProgress(forPublishProgress);

                                        predecessor = port;
                                    }
                                } else if (newNodeHash.compareTo(myPortHash) > 0) {
                                    String[] forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "predecessor",
                                            myPort
                                    };
                                    publishProgress(forPublishProgress);

                                    forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "successor",
                                            successor
                                    };
                                    publishProgress(forPublishProgress);

                                    forPublishProgress = new String[]{
                                            "adjustRing",
                                            successor,
                                            "predecessor",
                                            port
                                    };
                                    publishProgress(forPublishProgress);

                                    successor = port;
                                }
//                                msgPlusPortObject.predecessor = successor;
//                                msgPlusPortObject.successor = myPort;
//
////                                Req to succ to adjust its succ to port
////                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", successor, "successor", port);
//                                String[] forPublishProgress = new String[]{
//                                    "adjustRing",
//                                    successor,
//                                    "successor",
//                                    port
//                                };
//                                publishProgress(forPublishProgress);
//
//
//                                predecessor = port;
                            }
                        } else if (successor != predecessor) {
                            Log.d(TAG, "herererererere: " + myPort);
                            if (newNodeHash.compareTo(successorHash) > 0 && newNodeHash.compareTo(predecessorHash) > 0) {
                                Log.d(TAG, "1234: " + myPort);
                                if (myPortHash.compareTo(predecessorHash) > 0 && myPortHash.compareTo(successorHash) > 0) {
                                    Log.d(TAG, "apapapapapapapa: " + myPort);
//                                    msgPlusPortObject.predecessor = myPort;
                                    String[] forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "predecessor",
                                            myPort
                                    };
                                    publishProgress(forPublishProgress);
//                                    msgPlusPortObject.successor = successor;
                                    forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "successor",
                                            successor
                                    };
                                    publishProgress(forPublishProgress);



//                                    Req to succ to adjust its pred to port
//                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", successor, "predecessor", port);
                                    forPublishProgress = new String[]{
                                        "adjustRing",
                                        successor,
                                        "predecessor",
                                        port
                                    };
                                    publishProgress(forPublishProgress);

//I WAS HERE
                                    successor = port;
                                } else {
                                    Log.d(TAG, "uuuuuuuuuuu: " + successor +
                                            "    " + port);
//                                    Req "connect" to succ for port
//                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", successor, port);
                                    String[] forPublishProgress = new String[]{
//                                        "connectTo",
                                        "connect",
                                        successor,
                                        port
                                    };
                                    publishProgress(forPublishProgress);

                                }
                            } else if (newNodeHash.compareTo(successorHash) < 0 && newNodeHash.compareTo(predecessorHash) < 0) {
                                Log.d(TAG, "aaaaaaaaaaaa: " + myPort);
                                if (myPortHash.compareTo(predecessorHash) < 0 && myPortHash.compareTo(successorHash) < 0) {
                                    Log.d(TAG, "zmzmzmzmzmzmzmzmz: " + myPort);
//                                    msgPlusPortObject.successor = myPort;
                                    String[] forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "successor",
                                            myPort
                                    };
                                    publishProgress(forPublishProgress);
//                                    msgPlusPortObject.predecessor = predecessor;
                                    forPublishProgress = new String[]{
                                            "adjustRing",
                                            port,
                                            "predecessor",
                                            predecessor
                                    };
                                    publishProgress(forPublishProgress);

//                                    Req to pred to adjust its succ to port
//                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", predecessor, "successor", port);
                                    forPublishProgress = new String[]{
                                        "adjustRing",
                                        predecessor,
                                        "successor",
                                        port
                                    };
                                    publishProgress(forPublishProgress);

                                    predecessor = port;
                                } else {
                                    Log.d(TAG, "iiiiiiii: " + predecessor +
                                            "    " + port);
//                                    Req "connect" to pred for port
//                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", predecessor, port);
                                    String[] forPublishProgress = new String[]{
//                                        "connectTo",
                                        "connect",
                                        predecessor,
                                        port
                                    };
                                    publishProgress(forPublishProgress);
                                }
                            } else if (newNodeHash.compareTo(myPortHash) > 0 && newNodeHash.compareTo(successorHash) < 0) {
                                Log.d(TAG, "qqqqqqqqqqqqqqq: " + myPort);
                                msgPlusPortObject.successor = successor;
                                msgPlusPortObject.predecessor = myPort;

//                                Req to succ to adjust its pred to port
//                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", successor, "predecessor", port);
                                String[] forPublishProgress = new String[]{
                                    "adjustRing",
                                    successor,
                                    "predecessor",
                                    port
                                };
                                publishProgress(forPublishProgress);

                                successor = port;
                            } else if (newNodeHash.compareTo(myPortHash) < 0 && newNodeHash.compareTo(predecessorHash) > 0) {
                                Log.d(TAG, "tttttttttttttt: " + myPort);
                                msgPlusPortObject.successor = myPort;
                                msgPlusPortObject.predecessor = predecessor;

//                                Req to pred to adjust its succ to port
//                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", predecessor, "successor", port);
                                String[] forPublishProgress = new String[]{
                                    "adjustRing",
                                    predecessor,
                                    "successor",
                                    port
                                };
                                publishProgress(forPublishProgress);

                                predecessor = port;
                            }
                        }
//                        } else if (successorHash.compareTo(newNodeHash) >= 0 && myPortHash.compareTo(newNodeHash) < 0) {
//                            msgPlusPortObject.predecessor = myPort;
//                            msgPlusPortObject.successor = successor;
//
//                            successor = port;
//
//                            Log.d(TAG, " Succ in bw myPort: " + myPort + ", " +
//                                    "succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);
//
////                            out.writeObject(msgPlusPortObject);
////                            out.flush();
////                            out.close();
//                        } else if (predecessorHash.compareTo(newNodeHash) < 0 && myPortHash.compareTo(newNodeHash) >= 0) {
//                            msgPlusPortObject.predecessor = predecessor;
//                            msgPlusPortObject.successor = myPort;
//
//                            predecessor = port;
//
//                            Log.d(TAG, " pred in bw myPort: " + myPort + ", " +
//                                    "succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);
//
////                            out.writeObject(msgPlusPortObject);
////                            out.flush();
////                            out.close();
//                        } else if (!port.equals(CONNECT_PORT)) {
//                            Log.d(TAG,
//                                    " req to succ myPort: " + myPort + ", " + "succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);
//                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", successor, port);
//                            Log.d(TAG,
//                                    " req made to succ myPort: " + myPort +
//                                            ", " +
//                                            "succ: "+ successor + ", pred: "+predecessor + " reqFromPort " + port);
////                            out.writeObject(msgPlusPortObject);
////                            out.flush();
////                            out.close();
//                        }
//                        } else {
////                             Can't happen coz all join requests are handled by 5554 only
//                        }
                        out.writeObject(msgPlusPortObject);
                        out.flush();
                        out.close();
//                    } else if (msgPlusPortObject.msg.equals("connectTo")) {
//                        successor = msgPlusPortObject.port;
////                        predecessor = myPort;
//                        Log.d(TAG, "connectTo: successor"+successor+" " +
//                                "predecessor: "+predecessor);
//                        String[] forPublishProgress = new String[]{
//                                "connect",
//                                myPort,
//                                msgPlusPortObject.port
//                        };
//                        publishProgress(forPublishProgress);
                    } else if (msgPlusPortObject.msg.equals("adjustRing")) {
                        if (msgPlusPortObject.adjustNode.equals("successor")) {
                            successor = msgPlusPortObject.adjustNodeToPort;
                        } else if (msgPlusPortObject.adjustNode.equals("predecessor")) {
                            predecessor = msgPlusPortObject.adjustNodeToPort;
                        }

//                        out.writeObject(msgPlusPortObject);
//                        out.flush();
//                        out.close();
                    } else if (msgPlusPortObject.msg.equals("insert")) {
                        Log.d(TAG,
                                "ServerTask insert start from " + msgPlusPortObject.msg + " for " + myPort + " for key " + msgPlusPortObject.key);
                        ContentValues values = new ContentValues();
                        values.put(SimpleDhtProvider.COLUMN_NAME_KEY, msgPlusPortObject.key);
                        values.put(SimpleDhtProvider.COLUMN_NAME_VALUE, msgPlusPortObject.value);
//                        if (msgPlusPortObject.originator != null) {
//                            values.put("originator", msgPlusPortObject.originator);
//                        }
//                        Uri uri = getContentResolver().insert(SimpleDhtProvider.CONTENT_URI, values);
                        insert(CONTENT_URI, values);
                        Log.d(TAG, "ServerTask insert done from " + msgPlusPortObject.msg + " for " + myPort + " for key " + msgPlusPortObject.key);
                    } else if (msgPlusPortObject.msg.equals("search")) {
                        query(CONTENT_URI, null, msgPlusPortObject.key, null, null);
                    }

                    clientSocket.close();
                } catch (Exception e){
                    Log.e(TAG, "Server ExceptionFinal: " + e.toString());
                    e.printStackTrace();
//                    continue;
                }
            }
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

            if (strings[0].equals("adjustRing")) {
                Log.d(TAG, "onProgressUpdate adjustRing");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        "adjustRing", strings[1], strings[2], strings[3]);
//            } else if (strings[0].equals("connectTo")) {
//                Log.d(TAG, "onProgressUpdate connectTo");
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
//                        "connectTo", strings[1], strings[2]);
            } else if (strings[0].equals("connect")) {
                Log.d(TAG, "onProgressUpdate connect");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        "connect", strings[1], strings[2]);
            }

            return;
        }
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
        private final String SQL_CREATE_TABLE =
                "CREATE TABLE " + TABLE_NAME + " (" + COLUMN_NAME_KEY + " " +
                        "TEXT," + COLUMN_NAME_VALUE + " TEXT)";


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
}

class messageStruct implements Serializable {
    String msg, port, key, value, successor, predecessor, originator, adjustNode, adjustNodeToPort;

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

    public messageStruct(String message, String _originator, String _key, String _value){
        msg = message;
        originator = _originator;
        key = _key;
        value = _value;
    }

    public messageStruct(String message, String _adjustNode, String _adjustNodeToPort){
        msg = message;
        adjustNode = _adjustNode;
        adjustNodeToPort = _adjustNodeToPort;
    }
}
