package edu.buffalo.cse.cse486586.simpledht;

import java.net.Socket;
import java.net.InetAddress;
import java.net.ServerSocket;

import java.io.IOException;
import java.io.Serializable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.Formatter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
//import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;


import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.util.Log;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;

import android.content.Context;
import android.content.ContentValues;
import android.content.ContentProvider;

import android.database.Cursor;
import android.database.DatabaseUtils;
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
//    String originatorPort = null;

    String successor = null;
    String predecessor = null;
    String successorHash = null;
    String predecessorHash = null;


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

        if (successor == null) {
            db.insert(TABLE_NAME, null, values);
        } else if (successor == predecessor) {
            if (hashedKey.compareTo(myPortHash) <= 0 && hashedKey.compareTo(successorHash) < 0) {
                if (successorHash.compareTo(myPortHash) < 0) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"));
                } else {
                    db.insert(TABLE_NAME, null, values);
                }
            } else if (hashedKey.compareTo(myPortHash) > 0 && hashedKey.compareTo(successorHash) < 0) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"));
            } else if (hashedKey.compareTo(myPortHash) > 0 && hashedKey.compareTo(successorHash) > 0) {
                if (successorHash.compareTo(myPortHash) < 0) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"));
                } else {
                    db.insert(TABLE_NAME, null, values);
                }
            } else if (hashedKey.compareTo(myPortHash) < 0 && hashedKey.compareTo(successorHash) > 0) {
                db.insert(TABLE_NAME, null, values);
            }
        } else if (myPortHash.compareTo(hashedKey) >=
            0 && hashedKey.compareTo(predecessorHash) > 0) {
            db.insert(TABLE_NAME, null, values);
        } else if (myPortHash.compareTo(hashedKey) < 0 && predecessorHash.compareTo(hashedKey) < 0 && successorHash.compareTo(hashedKey) < 0 && myPortHash.compareTo(predecessorHash) < 0) {
            db.insert(TABLE_NAME, null, values);
        } else if (myPortHash.compareTo(hashedKey) > 0 && predecessorHash.compareTo(hashedKey) > 0 && successorHash.compareTo(hashedKey) > 0 && myPortHash.compareTo(predecessorHash) < 0) {
            db.insert(TABLE_NAME, null, values);
        } else {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, originalKey, (String) values.get("value"));
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


            if (!myPort.equals(CONNECT_PORT)) {
                // connect to 5554
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        "connect", CONNECT_PORT, myPort);
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

        String originatorPort = myPort;
        if (selectionArgs!= null && selectionArgs.length >= 1 && selectionArgs[0] != null) {
            originatorPort = selectionArgs[0];
        }

//        boolean searchRing = false;
//        if (selectionArgs!= null && selectionArgs.length >= 2 && selectionArgs[1] != null) {
//            searchRing = true;
//        }

        Log.d(TAG,
                "QUERY GLOBAL myPort: " + myPort + "," + " succ: "+ successor +
                        ", " + "pred: "+predecessor);

        Log.d(TAG, selection);
        if (selection.equals("@")) {
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
        } else if (selection.equals("*") || selection.equals("GDump")) {
            Log.d(TAG, "in * query myPort: " + myPort);
//            String[] selectionArgss = new String[]{selection};
//
//            selection = COLUMN_NAME_KEY + "=?";

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

            if (selection.equals("GDump")) {
                return cursor;
            }

            if (successor != null && !successor.equals(originatorPort)) {
                Log.d(TAG, "in query while loop for " + myPort + " to succ "+ successor);
                Cursor successorCursor = actSynchronously("search", successor, "*", originatorPort);

                cursor = new MergeCursor(new Cursor[]{cursor, successorCursor});
            }
//            return cursor;
        } else {
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

            Log.d("qKEY", selectionArgss[0]);
            try {
                if (cursor.getCount() <= 0) {
                    Log.d("cursor is Null", "-");
                } else {
                    Log.d("cursor is NOT Null", "-");
                    // Log.d("cursor is Null", DatabaseUtils.dumpCursorToString(cursor));
                    cursor.moveToFirst();
                    Log.d("qKEY cursor", cursor.getString(0));
                    Log.d("qVALUE cursor", cursor.getString(1));
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                e.printStackTrace();
            }

//            if (cursor.getCount() <= 0 && myPort.equals(originatorPort)) {
            if (cursor.getCount() <= 0) {
                try {
                    Log.d(TAG, "query ASYNC START");
//                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
//                            "search", successor, selectionArgss[0],
//                            originatorPort).get();

                    cursor = actSynchronously("search", successor, selectionArgss[0], originatorPort);

                    Log.d(TAG, "query ASYNC STOP");
                } catch (Exception e) {
                    Log.e(TAG, "first call to actSynchronously: " + myPort);
                    e.printStackTrace();
                }
//            } else {
//                return cursor;
            }
        }

        return cursor;
//        return null;
    }


    public Cursor actSynchronously(String msgType, String portToConnect, String key, String originatorPort) {
        try {
            Log.d(TAG, "actSynchronously Start for " + portToConnect);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));
            messageStruct msgStruct = new messageStruct(
                msgType,
                originatorPort,
                key,
                null,
                null
            );

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msgStruct);
            out.flush();


            Log.d(TAG, "actSynchronously waiting for ack");
            messageStruct ack = new messageStruct();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            ack = (messageStruct) in.readObject();

            Log.d(TAG, "actSynchronously ack rcvd");

            MatrixCursor matrixCursor;
            matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});

            if (!key.equals("*")) {
                MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
                if (!ack.value.equals(null)) {
                    newRow.add(ack.key);
                    newRow.add(ack.value);

                    return matrixCursor;
                }
            } else {
                Log.d(TAG, "actSynchronously after ack rcvd in * else");

//                Map<String, String> cursorKeyValueMap = (HashMap<String, String>) ack.keyValueMap;
                Map<String, String> cursorKeyValueMap = ack.keyValueMap;

                if (cursorKeyValueMap != null && !cursorKeyValueMap.isEmpty()) {
                    Log.d(TAG, "actSynchronously after ack rcvd in * else b4 " +
                            "for loop pack in matrixcursor");
                    for (Map.Entry<String, String> entry : cursorKeyValueMap.entrySet()) {
                        MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
                        Log.d(TAG, "key: "+entry.getKey());
                        Log.d(TAG, "value: "+entry.getValue());
                        newRow.add(entry.getKey());
                        newRow.add(entry.getValue());
                    }
                }

                Log.d(TAG, "actSynchronously after ack rcvd in * else before " +
                        "return");
                return matrixCursor;
            }

            in.close();
            socket.close();
        } catch (Exception e) {
            Log.e(TAG, "query Client search ExceptionFinal: " + e.toString());
            e.printStackTrace();
        }

        return null;
    }


















    // TRY CURSOR AS 2ND PARAM FOR ONPROGRESSUPDATE
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            messageStruct ack = new messageStruct();

            String msgType = msgs[0];
            String nxtSuccessor = msgs[1];

            if (msgType.equals("connect")) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        msgs[2]
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    out.writeObject(msgStruct);
                    out.flush();

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ack = (messageStruct) in.readObject();

                    if (ack.successor != null && ack.successor.length() > 0) {
                        successor = ack.successor;
                    }
                    if (ack.predecessor != null && ack.predecessor.length() > 0) {
                        predecessor = ack.predecessor;
                    }

                    in.close();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Client connect ExceptionFinal: " + e.toString());
                    e.printStackTrace();
                }
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

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        null,
                        key,
                        value
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    out.writeObject(msgStruct);
                    out.flush();

                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Client insert ExceptionFinal: " + e.toString());
                    e.printStackTrace();
                }
            } else if (msgType.equals("search")) {
                String key = msgs[2];
                String originatorPort = msgs[3];

                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));

                    messageStruct msgStruct = new messageStruct(
                        msgType,
                        originatorPort,
                        key,
                        null
                    );

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    out.writeObject(msgStruct);
                    out.flush();


//                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
//                    ack = (messageStruct) in.readObject();

//                    if (ack.value.equals("KEY_NOT_FOUND")) {
//
//                    }

//                    in.close();
                    socket.close();
                } catch (Exception e) {
                    Log.e(TAG, "Client search ExceptionFinal: " + e.toString());
                    e.printStackTrace();
                }
            }

            return null;
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            messageStruct msgPlusPortObject = new messageStruct();

            while(true){
                Log.d(TAG, "GLOBAL myPort: " + myPort + "," + " succ: "+ successor + ", pred: "+predecessor);
                try {
                    Socket clientSocket = null;
                    clientSocket = serverSocket.accept();

                    ObjectInputStream in = null;
                    in = new ObjectInputStream(clientSocket.getInputStream());

                    msgPlusPortObject = (messageStruct) in.readObject();

                    if (msgPlusPortObject.msg.equals("connect")) {
                        String newNodeHash = null;
                        String port = msgPlusPortObject.port;
                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());

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


                        if (successor == null) {
                            predecessor = port;
                            successor = port;

                            msgPlusPortObject.successor = CONNECT_PORT;
                            msgPlusPortObject.predecessor = CONNECT_PORT;
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
                            }
                        } else if (successor != predecessor) {
                            if (newNodeHash.compareTo(successorHash) > 0 && newNodeHash.compareTo(predecessorHash) > 0) {
                                if (myPortHash.compareTo(predecessorHash) > 0 && myPortHash.compareTo(successorHash) > 0) {
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
                                        "connect",
                                        successor,
                                        port
                                    };
                                    publishProgress(forPublishProgress);

                                }
                            } else if (newNodeHash.compareTo(successorHash) < 0 && newNodeHash.compareTo(predecessorHash) < 0) {
                                if (myPortHash.compareTo(predecessorHash) < 0 && myPortHash.compareTo(successorHash) < 0) {
                                    String[] forPublishProgress = new String[]{
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

                                    forPublishProgress = new String[]{
                                        "adjustRing",
                                        predecessor,
                                        "successor",
                                        port
                                    };
                                    publishProgress(forPublishProgress);

                                    predecessor = port;
                                } else {
                                    String[] forPublishProgress = new String[]{
                                        "connect",
                                        predecessor,
                                        port
                                    };
                                    publishProgress(forPublishProgress);
                                }
                            } else if (newNodeHash.compareTo(myPortHash) > 0 && newNodeHash.compareTo(successorHash) < 0) {
                                msgPlusPortObject.successor = successor;
                                msgPlusPortObject.predecessor = myPort;

                                String[] forPublishProgress = new String[]{
                                    "adjustRing",
                                    successor,
                                    "predecessor",
                                    port
                                };
                                publishProgress(forPublishProgress);

                                successor = port;
                            } else if (newNodeHash.compareTo(myPortHash) < 0 && newNodeHash.compareTo(predecessorHash) > 0) {
                                msgPlusPortObject.successor = myPort;
                                msgPlusPortObject.predecessor = predecessor;

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
                        out.writeObject(msgPlusPortObject);
                        out.flush();
                        out.close();
                    } else if (msgPlusPortObject.msg.equals("adjustRing")) {
                        if (msgPlusPortObject.adjustNode.equals("successor")) {
                            successor = msgPlusPortObject.adjustNodeToPort;
                        } else if (msgPlusPortObject.adjustNode.equals("predecessor")) {
                            predecessor = msgPlusPortObject.adjustNodeToPort;
                        }
                    } else if (msgPlusPortObject.msg.equals("insert")) {
                        ContentValues values = new ContentValues();

                        values.put(SimpleDhtProvider.COLUMN_NAME_KEY, msgPlusPortObject.key);
                        values.put(SimpleDhtProvider.COLUMN_NAME_VALUE, msgPlusPortObject.value);

                        insert(CONTENT_URI, values);
                    } else if (msgPlusPortObject.msg.equals("search")) {
                        Log.d(TAG, "Server search key: " + msgPlusPortObject.key);


                        Cursor cursor = null;
                        boolean asyncReq = false;
                        String hashKey = null;
                        String[] forPublishProgress;

//                        if (!msgPlusPortObject.key.equals("*")) {
//                            try {
//                                hashKey = genHash(msgPlusPortObject.key);
//                            } catch (NoSuchAlgorithmException e) {
//                                Log.e(TAG, "CP query " + e.toString());
//                            }
//                        }

//                        try {
//                            if (successor != null) {
//                                successorHash = genHash(String.valueOf((Integer.parseInt(successor) / 2)));
//                            }
//                        } catch (NoSuchAlgorithmException e) {
//                            Log.e(TAG, "successorHash " + e.toString());
//                        }
//
//                        try {
//                            if (predecessor != null) {
//                                predecessorHash = genHash(String.valueOf((Integer.parseInt(predecessor) / 2)));
//                            }
//                        } catch (NoSuchAlgorithmException e) {
//                            Log.e(TAG, "predecessorHash " + e.toString());
//                        }

                        if (!msgPlusPortObject.key.equals("*")) {
                            try {
                                hashKey = genHash(msgPlusPortObject.key);
                            } catch (NoSuchAlgorithmException e) {
                                Log.e(TAG, "CP query " + e.toString());
                            }
                            if (predecessor == null || (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessorHash) > 0)) {
                                cursor = query(CONTENT_URI, null,
                                        msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                            } else if (predecessor == successor) {
                                if (hashKey.compareTo(myPortHash) <= 0 && hashKey.compareTo(successorHash) < 0) {
                                    cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                                } else if (hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) < 0) {
//                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
//                                asyncReq = true;
//                                forPublishProgress = new String[]{
//                                    "search",
//                                    successor,
//                                    msgPlusPortObject.key,
//                                    msgPlusPortObject.originatorPort
//                                };
//                                publishProgress(forPublishProgress);
                                    cursor = actSynchronously("search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
                                } else if (hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) > 0) {
                                    cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                                }
                            } else if (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessorHash) > 0) {
                                cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                            } else if (myPortHash.compareTo(hashKey) < 0 && predecessorHash.compareTo(hashKey) < 0 && successorHash.compareTo(hashKey) < 0 && myPortHash.compareTo(predecessorHash) < 0) {
                                cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                            } else if (myPortHash.compareTo(hashKey) > 0 && predecessorHash.compareTo(hashKey) > 0 && successorHash.compareTo(hashKey) > 0 && myPortHash.compareTo(predecessorHash) < 0) {
                                cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                            } else {
//                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
//                            asyncReq = true;
//                            forPublishProgress = new String[]{
//                                "search",
//                                successor,
//                                msgPlusPortObject.key,
//                                msgPlusPortObject.originatorPort
//                            };
//                            publishProgress(forPublishProgress);
                                cursor = actSynchronously("search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
                            }
                            if (cursor.getCount() > 0) {
                                // CHECK IF THIS WORKS GOOD FOR * AND @
                                cursor.moveToFirst();
                                msgPlusPortObject.value = cursor.getString(1);
                            }
                        } else {
//                            cursor = actSynchronously("search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
                            Log.d(TAG, "before query here on server serach for *");
                            cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
                            Log.d(TAG, "after query here on server serach for" +
                                    " *");
                            if (cursor.getCount() > 0) {
                                Log.d(TAG, "after query here on server serach for * cursor have data");
                                // CHECK IF THIS WORKS GOOD FOR * AND @
                                cursor.moveToFirst();

//                                ArrayList<String> cursorArrayList = new ArrayList<String>();
//                                for(cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
//                                    cursorArrayList.add(cursor.getString(0));
//                                }
                                Map<String, String> cursorKeyValueMap = new HashMap<String, String>();
                                while(!cursor.isAfterLast()) {
                                    cursorKeyValueMap.put(cursor.getString(0), cursor.getString(1));
                                    cursor.moveToNext();
                                }
                                Log.d(TAG, "after query here on server serach" +
                                        " for * cursor have data packed in " +
                                        "map");
                                msgPlusPortObject.keyValueMap = cursorKeyValueMap;
                            }
                        }

//                        if (!asyncReq) {
//                            Log.d(TAG, "not async req");
//                            if (cursor.getCount() <= 0){
//                                Log.d(TAG, "cursor is null");
//                            } else {
//                                cursor.moveToFirst();
//                                forPublishProgress = new String[]{
//                                    "searchResult",
//                                    msgPlusPortObject.originatorPort,
//                                    cursor.getString(0),
//                                    cursor.getString(1),
//                                };
//                                publishProgress(forPublishProgress);
//                            }
//                        }

//                        if (cursor.getCount() <= 0){
//                            Log.d(TAG, "cursor is null");
//                        } else {
//                            cursor = actSynchronously("search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
//                        }

//                        if (cursor != null) {
//                            cursor.moveToFirst();
//
//                            msgPlusPortObject.key = cursor.getString(0);
//                            msgPlusPortObject.value = cursor.getString(1);
//                        } else {
////                            msgPlusPortObject.value = "KEY_NOT_FOUND";
//                            String[] forPublishProgress = new String[]{
//                                "searchResult",
//                                msgPlusPortObject.originatorPort,
//                                cursor.getString(0),
//                                cursor.getString(1)
//                            };
//                            publishProgress(forPublishProgress);
//                        }




                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                        out.writeObject(msgPlusPortObject);
                        out.flush();
                        out.close();
                    } else if (msgPlusPortObject.msg.equals("searchResult")) {

                    }

                    clientSocket.close();
                } catch (Exception e){
                    Log.d(TAG, "Server search Error: " + e.toString());
                    e.printStackTrace();
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
            } else if (strings[0].equals("connect")) {
                Log.d(TAG, "onProgressUpdate connect");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        "connect", strings[1], strings[2]);
            } else if (strings[0].equals("search")) {
                Log.d(TAG, "onProgressUpdate search");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "search", strings[1], strings[2], strings[3]);
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
    String msg, port, key, value, successor, predecessor, originatorPort, adjustNode, adjustNodeToPort;
    Map<String, String> keyValueMap;

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

    public messageStruct(String message, String _adjustNode, String _adjustNodeToPort){
        msg = message;
        adjustNode = _adjustNode;
        adjustNodeToPort = _adjustNodeToPort;
    }

    public messageStruct(String message, String _originatorPort, String _key, String _value){
        msg = message;
        originatorPort = _originatorPort;
        key = _key;
        value = _value;
    }

    public messageStruct(String message, String _originatorPort, String _key,
                         String _value, Map<String, String> _keyValueMap){
        msg = message;
        key = _key;
        value = _value;
        keyValueMap = _keyValueMap;
        originatorPort = _originatorPort;
    }
}
