package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	public static final String TABLE_NAME = "messages";
	public static final String COLUMN_NAME_KEY = "key";
	public static final String COLUMN_NAME_VALUE = "value";
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	static final int SERVER_PORT = 10000;
	static final String CONNECT_PORT = "";
	String myPort = null;
	String portStr = null;
	boolean INSERTION = false;
	String myPortHash = null;
	String successor = null;
	String predecessor = null;
	String successorHash = null;
	String predecessorHash = null;
	Map<String, String> successorMap = new HashMap<String, String>();
	Map<String, String> predecessorMap = new HashMap<String, String>();
	Map<String, String> portHashMap = new HashMap<String, String>();
	ArrayList<String> list1 = new ArrayList<String>();
	ArrayList<String> list2 = new ArrayList<String>();
	ArrayList<String> list3 = new ArrayList<String>();
	ArrayList<String> list4 = new ArrayList<String>();
	Map<ArrayList<String>, String> hashedKeyToNodeMap = new HashMap<ArrayList<String>, String>();
	private SQLiteDatabase db;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Log.d(TAG, "delete key: " + selection);
		String hashedKey = null;
		try {
			hashedKey = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "CP delete hashedKey originalKey: " + selection + " " + e.toString());
		}

		String[] selectionArgss = new String[]{selection};
		selection = COLUMN_NAME_KEY + "=?";

		db.delete(TABLE_NAME, selection, selectionArgss);
		Log.d(TAG, "key deleted locally: " + selectionArgss[0]);

		String portToStoreKey = getNodeToStoreKey(hashedKey);
		if (myPort.equals(portToStoreKey)) {

			String port = portToStoreKey;
			for (int i = 0; i < 2; i++) {
				port = successorMap.get(port);

				Log.d(TAG, "calling succ: " + port + " to delete from:" + myPort);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", port, selectionArgss[0]);
			}
		}

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
		INSERTION = true;
		String hashedKey = null;
		try {
			hashedKey = genHash(originalKey);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "CP insert hashedKey " + e.toString());
		}

		String portToStoreKey = getNodeToStoreKey(hashedKey);

		// Log.d(TAG, "INSERTION " + myPort + " " + originalKey);
		if (myPort.equals(portToStoreKey)) {
			// Log.d(TAG, "INSERTION myPort.equals(portToStoreKey) " + myPort + " " + originalKey);
			ContentValues newValues = new ContentValues();

			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
			newValues.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, (String) values.get("value"));
			newValues.put("type", "insertion");
			newValues.put("port", myPort);
			int updated = db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY+"=?", new String[]{(String) values.get("key")});
			if (updated == 0) {
				db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
			}
//			db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_IGNORE);
//			db.insert(TABLE_NAME, null, values);

			// REPLICATION
			String port = myPort;
			for (int i=0; i<2; i++) {
				port = successorMap.get(port);

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", port, originalKey, (String) values.get("value"), myPort);
			}

		} else {
			// Log.d(TAG, "INSERTION myPort Not equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + originalKey);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", portToStoreKey, originalKey, (String) values.get("value"));
		}
		INSERTION = false;


		return uri;
	}

	public Uri insertReplication(Uri uri, ContentValues values) {
		// Log.d(TAG, "insertReplicationFunc: " + " key: "+ (String) values.get("key") + " value: "+ (String) values.get("value") + " type: "+ (String) values.get("type") + " port: "+ (String) values.get("port") + " myPort: " + myPort);
		INSERTION = true;
		int updated = db.update(TABLE_NAME, values, COLUMN_NAME_KEY+"=?", new String[]{(String) values.get("key")});
		if (updated == 0) {
			db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
		}
		INSERTION = false;
//		db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_IGNORE);
//		db.insert(TABLE_NAME, null, values);

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, e.toString());
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		try {
			successorMap.put("11108", "11116");
			successorMap.put("11116", "11120");
			successorMap.put("11120", "11124");
			successorMap.put("11124", "11112");
			successorMap.put("11112", "11108");

			predecessorMap.put("11108", "11112");
			predecessorMap.put("11112", "11124");
			predecessorMap.put("11124", "11120");
			predecessorMap.put("11120", "11116");
			predecessorMap.put("11116", "11108");

			portHashMap.put("11108", "33d6357cfaaf0f72991b0ecd8c56da066613c089");
			portHashMap.put("11112", "208f7f72b198dadd244e61801abe1ec3a4857bc9");
			portHashMap.put("11116", "abf0fd8db03e5ecb199a9b82929e9db79b909643");
			portHashMap.put("11120", "c25ddd596aa7c81fa12378fa725f706d54325d12");
			portHashMap.put("11124", "177ccecaec32c54b82d5aaafc18a2dadb753e3b1");


			// > 11124 & <= 11112
			list1.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
			list1.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
			hashedKeyToNodeMap.put(list1, "11112");

			// > 11112 & <= 11108
			list2.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
			list2.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
			hashedKeyToNodeMap.put(list2, "11108");

			// > 11108 & <= 11116
			list3.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
			list3.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
			hashedKeyToNodeMap.put(list3, "11116");

			// > 11116 & <= 11120
			list4.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
			list4.add("c25ddd596aa7c81fa12378fa725f706d54325d12");
			hashedKeyToNodeMap.put(list4, "11120");

			// everything else will go to 11124


			SimpleDynamoDBHelper DBHelper = new SimpleDynamoDBHelper(getContext());

			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myPort = String.valueOf((Integer.parseInt(portStr) * 2));

			Log.d(TAG, "CP onCreate: " + myPort);

			myPortHash = portHashMap.get(myPort);

			successor = successorMap.get(myPort);
			successorHash = portHashMap.get(successor);

			predecessor = predecessorMap.get(myPort);
			predecessorHash = portHashMap.get(predecessor);


			db = DBHelper.getWritableDatabase();
			if (db != null) {

				String port = myPort;
				for (int i = 0; i < 2; i++) {
					port = predecessorMap.get(port);

					// Log.d(TAG, "calling pred: " + port + " to replicate from:" + " " + myPort);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replicate", port, myPort);
				}

				port = myPort;
				for (int i = 0; i < 2; i++) {
					port = successorMap.get(port);
					// Log.d(TAG, "calling succ: " + port + " to getMissedInsert from:" + myPort);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "getMissedInsert", port, myPort);
				}
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
		Cursor cursor = null;
		Cursor dummyCursor = null;

		while(INSERTION) {
			Log.d(TAG, "score INSERTION BEING DONE FOR KEY: " + selection);
		}

		String originatorPort = myPort;
		if (selectionArgs!= null && selectionArgs.length >= 1 && selectionArgs[0] != null) {
			originatorPort = selectionArgs[0];
		}
		Log.d(TAG, "qKEY selection:" + selection + " myPort: "+ myPort + " " + "originatorPort: " + originatorPort);

		projection = new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE};


		Log.d(TAG, "replicate query insertion 1");
		Log.d(TAG, "QUERY GLOBAL myPort: " + myPort + ", succ: "+ successor + ", pred: "+predecessor);

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
			Log.d(TAG, "query: @");
			dummyCursor = db.query(
					TABLE_NAME,
					null,
					null,
					null,
					null,
					null,
					sortOrder,
					null
			);
			Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
			Log.d(TAG, "qKEY " + selection);
		} else if (selection.equals("*") || selection.equals("GDump") || selection.equals("insertion") || selection.equals("replication")) {
			Log.d(TAG, "in * query myPort: " + myPort);

			String dupSelection = selection;
			Log.d(TAG, "replicate query insertion 2");
			if (selection.equals("insertion") || selection.equals("replication")) {
				Log.d(TAG, "replicate query insertion 3");

				String[] selectionArgss = null;
				if (selection.equals("insertion")) {
					selectionArgss = new String[]{selection};
					selection = "type=?";
				} else if (selection.equals("replication")) {
					selectionArgss = new String[]{selection, originatorPort};
					selection = "type=? AND port=?";
				}

				Log.d(TAG, "replicate query insertion 4");

				try {
					cursor = db.query(
							TABLE_NAME,
							projection,
							selection,
							selectionArgss,
							null,
							null,
							sortOrder,
							null
					);
					dummyCursor = db.query(
							TABLE_NAME,
							null,
							selection,
							selectionArgss,
							null,
							null,
							sortOrder,
							null
					);
					Log.d(TAG, "replicate query insertion 5");
				} catch (Exception e) {
					Log.e(TAG,
							"Query ERROR " + selection + " " + selectionArgss[0]);
					// Log.d(TAG, "Query ERROR " + selection + " " + selectionArgss[0] + " " + selectionArgss[1]);
				}
			} else {
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
				dummyCursor = db.query(
						TABLE_NAME,
						null,
						null,
						null,
						null,
						null,
						sortOrder,
						null
				);
			}

			if (selection.equals("GDump") || dupSelection.equals("insertion") || dupSelection.equals("replication")) {
				Log.d(TAG, "replicate query insertion 6");
				Log.d(TAG, "query " + dupSelection);
				Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
//				cursor.moveToFirst();
				return cursor;
			}

//			if (successor != null && !successor.equals(originatorPort)) {
				Log.d(TAG, "successor != null && !successor.equals" +
						"(originatorPort) " + myPort + " to succ "+ successor + " originatorPort: "+originatorPort);
				String succ = successor;
				Cursor successorCursor = null;
				while (!succ.equals(myPort)) {
					try {

						successorCursor = actSynchronously("search", succ,
								"@", originatorPort);
						cursor = new MergeCursor(new Cursor[]{cursor, successorCursor});

						succ = successorMap.get(succ);
					} catch (Exception e) {
						Log.e(TAG, "successor != null && !successor.equals" +
								"(originatorPort) CATCH: " + e.toString());
						continue;
					}
				}
//			}
			Log.d(TAG, "query *");
			dummyCursor = cursor;
			Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
		} else {
			String hashedKey = null;
			try {
				hashedKey = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "CP insert hashedKey " + e.toString());
			}

			String[] selectionArgss = new String[]{selection};
			String portToStoreKey = getNodeToStoreKey(hashedKey);

			Log.d(TAG, "QUERYING " + myPort + " " + selection);


			Log.d(TAG, "QUERYING myPort.equals(portToStoreKey) " + myPort + " " + selection);

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
			dummyCursor = db.query(
					TABLE_NAME,
					null,
					selection,
					selectionArgss,
					null,
					null,
					sortOrder,
					"1"
			);

				Log.d(TAG, "qKEY " + selectionArgss[0]);
			try {
				if (cursor.getCount() > 0) {
					Log.d(TAG, "query cursor myPort.equals" +
							"(portToStoreKey) ");
					Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
					cursor.moveToFirst();
					Log.d(TAG, "cursor qKEY " + cursor.getString(0));
					Log.d(TAG, "cursor qVALUE " + cursor.getString(1));
					cursor.moveToFirst();
				}
//				} else {
					// TODO
					//HERERERERERERERERRERERER CHECK IF KEY IS REPLICATED LOCALLY
					// OR JUST QUERY LOCAL, IF NOT FOUND THEN SEARCH THE RING
					Log.d(TAG, "QUERYING myPort Not equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + selection);
//					while ((cursor != null && cursor.getCount() <= 0) || cursor == null) {
//					while (cursor == null || cursor.getCount() <= 0) {


//				int LOOP = 3;
				String port = portToStoreKey;

//				if (myPort.equals(port)) {
//					LOOP = 2;
//					port = successorMap.get(port);
//					Log.d(TAG, "query calling succ: " + port + " to " + "actSynchronously search from SAME PORT:" + myPort);
//				}
//				for (int i = 0; i < LOOP; i++) {

//				if (cursor.getCount() > 0) {
//					port = myPort;
//				}

				if (((cursor != null && cursor.getCount() <= 0) || cursor == null) && !myPort.equals(originatorPort)) {
					return cursor;
				}

				if (portToStoreKey.equals(myPort) || cursor.getCount() > 0) {
					Log.d(TAG, "(portToStoreKey.equals(myPort) || cursor" +
							".getCount() > 0)");
					port = successorMap.get(myPort);
				} else if (portToStoreKey.equals(originatorPort)) {
					Log.d(TAG, "continuing in query coz port.equals" +
							"(originatorPort)");
//						continue;
					port = successorMap.get(portToStoreKey);
				}

				if (port.equals(originatorPort)) {
					return cursor;
				}


					Log.d(TAG, "query calling succ: " + port + " to " +
							"actSynchronously search from: " + myPort);

					try {
						Cursor cursorReturned = null;
						cursorReturned = actSynchronously("search",
								port, selectionArgss[0], originatorPort);
						Log.d(TAG, "query cursor myPort Not equals " +
								"portToStoreKey): " + portToStoreKey);
						dummyCursor = cursorReturned;
						Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));

//							while (cursor.getCount() <= 0) {
//								portToStoreKey = successorMap.get(portToStoreKey);
//								cursor = actSynchronously("search", portToStoreKey, selectionArgss[0], originatorPort);
//							}
						if (cursorReturned != null && cursorReturned.getCount() >= 1) {
							cursor = cursorReturned;
						}
//					cursor.moveToFirst();
					} catch (Exception e) {
						Log.e(TAG, "first call to actSynchronously: " + myPort);
						e.printStackTrace();
//							portToStoreKey = successorMap.get(portToStoreKey);
//							continue;
					}

//					port = successorMap.get(port);
//				}
//				}
			} catch (Exception e) {
				Log.e(TAG, "querying else " + e.toString());
				e.printStackTrace();
			}

//			=================================
//			if (myPort.equals(portToStoreKey)) {
////				Log.d(TAG, "QUERYING myPort.equals(portToStoreKey) " + myPort + " " + selection);
//
//				selection = COLUMN_NAME_KEY + "=?";
//
//				cursor = db.query(
//					TABLE_NAME,
//					projection,
//					selection,
//					selectionArgss,
//					null,
//					null,
//					sortOrder,
//					"1"
//				);
//				dummyCursor = db.query(
//						TABLE_NAME,
//						null,
//						selection,
//						selectionArgss,
//						null,
//						null,
//						sortOrder,
//						"1"
//				);
//
////				Log.d(TAG, "qKEY " + selectionArgss[0]);
//				try {
//					if (cursor.getCount() > 0) {
//						Log.d(TAG, "query cursor myPort.equals" +
//								"(portToStoreKey) ");
//						Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
////						cursor.moveToFirst();
////						Log.d(TAG,"cursor qKEY " + cursor.getString(0));
////						Log.d(TAG,"cursor qVALUE "+ cursor.getString(1));
//					}
//				} catch (Exception e) {
//					Log.e(TAG, e.toString());
//					e.printStackTrace();
//				}
//
//			} else {
//				// TODO
//				//HERERERERERERERERRERERER CHECK IF KEY IS REPLICATED LOCALLY
//				// OR JUST QUERY LOCAL, IF NOT FOUND THEN SEARCH THE RING
//				Log.d(TAG, "QUERYING myPort Not equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + selection);
//				try {
//					cursor = actSynchronously("search", portToStoreKey, selectionArgss[0], originatorPort);
//					Log.d(TAG, "query cursor myPort Not equals" +
//							"(portToStoreKey) ");
//					dummyCursor = cursor;
//					Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
////					cursor.moveToFirst();
//				} catch (Exception e) {
//					Log.e(TAG, "first call to actSynchronously: " + myPort);
//					e.printStackTrace();
//				}
//			}
		}

		return cursor;
	}

	public Cursor actSynchronously(String msgType, String portToConnect, String key, String originatorPort) {
		try {
			Log.d(TAG, "actSynchronously Start for msgType:" + msgType + " " + "portToConnect: " + portToConnect + " key: "+key+ " originatorPort:" + originatorPort);
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


			messageStruct ack = new messageStruct();
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			ack = (messageStruct) in.readObject();


			MatrixCursor matrixCursor;
			matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});

			if (!(key.equals("*") || key.equals("@"))) {
				MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
				if (!ack.value.equals(null)) {
					newRow.add(ack.key);
					newRow.add(ack.value);

					return matrixCursor;
				}
			} else {
				Map<String, String> cursorKeyValueMap = ack.keyValueMap;

				if (cursorKeyValueMap != null && !cursorKeyValueMap.isEmpty()) {
					for (Map.Entry<String, String> entry : cursorKeyValueMap.entrySet()) {
						MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
						newRow.add(entry.getKey());
						newRow.add(entry.getValue());
					}
				}

				return matrixCursor;
			}

			in.close();
			socket.close();
		} catch (Exception e) {
			Log.e(TAG,
					"query Client search ExceptionFinal: " + e.toString() +
							" actSynchronously Start for msgType:" + msgType + " " + "portToConnect: " + portToConnect + " key: "+key+ " originatorPort:" + originatorPort);
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getNodeToStoreKey(String hashedKey) {
		ArrayList<String> list;

		for (Map.Entry<ArrayList<String>, String> entry : hashedKeyToNodeMap.entrySet()) {
			list = entry.getKey();

			if (hashedKey.compareTo(list.get(0)) > 0 && hashedKey.compareTo(list.get(1)) <= 0) {
				return entry.getValue();
			}
		}

		return "11124";
	}

//	================================================================================

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class ClientTask extends AsyncTask<String, String, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			messageStruct ack = new messageStruct();

			String msgType = msgs[0];
			String nxtSuccessor = msgs[1];

			if (msgType.equals("insert") || msgType.equals("replication")) {
				String key = msgs[2];
				String value = msgs[3];

				Log.d(TAG, msgType + " ClienTask for key: " + key + " myPort: " + myPort + " sent to: " + nxtSuccessor);
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
					socket.setSoTimeout(100);

					Log.d(TAG, msgType + " ClienTask for key: " + key + " " + "myPort: " + myPort + " sent to: " + nxtSuccessor + " DUPLICATE MSG");
					messageStruct msgStruct;
					if (msgType.equals("replication")) {
						msgStruct = new messageStruct(
							msgType,
							msgs[4],
							key,
							value
						);
					} else {
						msgStruct = new messageStruct(
							msgType,
							null,
							key,
							value
						);
					}

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgStruct);
					out.flush();

					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					ack = (messageStruct) in.readObject();
					in.close();

					socket.close();
//					Log.d(TAG, msgType + " ClienTask for key: " + key + " " + "myPort: " + myPort + " sent to: " + nxtSuccessor + " DUPLICATE MSG SENT");
				} catch (EOFException e) {
					Log.e(TAG, "ClientTask " + msgType + " " + nxtSuccessor + " SocketTimeoutException " + key + " : " + e.toString());

					String[] forClientPublishProgress = null;
					if (msgType.equals("insert")) {
						String port = nxtSuccessor;

						for (int i=0; i<2; i++) {
							port = successorMap.get(port);

							forClientPublishProgress = new String[]{
								"replication",
								port,
								key,
								value,
								nxtSuccessor
							};

							publishProgress(forClientPublishProgress);
						}
//					} else if (msgType.equals("replication")) {
//						String port = nxtSuccessor;
//						String originatorPort = msgs[4];
//
//						for (int i=0; i<2; i++) {
//							port = successorMap.get(port);
//
//							forClientPublishProgress = new String[]{
//								"replication",
//								port,
//								key,
//								value,
//								nxtSuccessor
//							};
//						}
					}
				} catch (Exception e) {
					Log.e(TAG,
							"Client " + msgType + " ExceptionFinal: " + nxtSuccessor + " " + e.toString());
					e.printStackTrace();
				}
			} else if (msgType.equals("delete")) {
				String key = msgs[2];
//				String originatorPort = msgs[3];

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));

					messageStruct msgStruct = new messageStruct(
							msgType,
							null,
							key,
							null
					);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgStruct);
					out.flush();

					socket.close();
				} catch (Exception e) {
					Log.e(TAG, "Client delete ExceptionFinal: " + e.toString());
					e.printStackTrace();
				}
			} else if (msgType.equals("replicate") || msgType.equals("getMissedInsert")) {
				String originatorPort = msgs[2];

//				Log.d(TAG, "Insert ClienTask for key: " + key + " myPort: " + myPort + " sent to: " + nxtSuccessor);
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));

					messageStruct msgStruct = new messageStruct(
						msgType,
						originatorPort,
						null,
						null
					);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgStruct);
					out.flush();

					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					ack = (messageStruct) in.readObject();

					Map<String, String> cursorKeyValueMap = ack.keyValueMap;

					if (cursorKeyValueMap != null && !cursorKeyValueMap.isEmpty()) {
						for (Map.Entry<String, String> entry : cursorKeyValueMap.entrySet()) {
							Log.d(TAG,
									"looping "+msgType+" for failed nodes " + entry.getKey() + " myPort: " + myPort + " from node: " + nxtSuccessor);
							ContentValues values = new ContentValues();

							values.put(SimpleDynamoProvider.COLUMN_NAME_KEY, entry.getKey());
							values.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, entry.getValue());

							if (msgType.equals("getMissedInsert")) {
								values.put("type", "insertion");
								values.put("port", myPort);
							} else if (msgType.equals("replicate")) {
								values.put("type", "replication");
								values.put("port", nxtSuccessor);
							}
							insertReplication(CONTENT_URI, values);
						}
					}

					in.close();

					socket.close();
				} catch (Exception e) {
					Log.e(TAG, "Client " + msgType + " ExceptionFinal: " + e.toString());
					e.printStackTrace();
				}
			}

			return null;
		}

		protected void onProgressUpdate(String... strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */

			if (strings[0].equals("replication")) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", strings[1], strings[2], strings[3], strings[4]);
//			} else if (strings[0].equals("connect")) {
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", strings[1], strings[2]);
//			} else if (strings[0].equals("search")) {
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "search", strings[1], strings[2], strings[3]);
			}

			return;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			messageStruct msgPlusPortObject = new messageStruct();

			while(true){
//				Log.d(TAG, "GLOBAL myPort: " + myPort + "," + " succ: "+ successor + ", pred: "+predecessor);
				try {
					Socket clientSocket = null;
					clientSocket = serverSocket.accept();

					ObjectInputStream in = null;
					in = new ObjectInputStream(clientSocket.getInputStream());

					msgPlusPortObject = (messageStruct) in.readObject();

					if (msgPlusPortObject.msg.equals("insert") || msgPlusPortObject.msg.equals("replication")) {
						ContentValues values = new ContentValues();

						values.put(SimpleDynamoProvider.COLUMN_NAME_KEY, msgPlusPortObject.key);
						values.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, msgPlusPortObject.value);

						if (msgPlusPortObject.msg.equals("replication")) {
							Log.d(TAG, "insertReplication ServerTask for key:" + " " + msgPlusPortObject.key + " myPort: " + myPort);
							values.put("type", "replication");
							values.put("port", msgPlusPortObject.originatorPort);
							insertReplication(CONTENT_URI, values);
						} else {

							Log.d(TAG, "Insert ServerTask for key: " + msgPlusPortObject.key + " myPort: " + myPort);
							values.put("type", "insertion");
							values.put("port", myPort);
							insert(CONTENT_URI, values);
//							Log.d(TAG, "Insert DONE ServerTask for key: " + msgPlusPortObject.key + " myPort: " + myPort);
						}
						ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
						out.writeObject(msgPlusPortObject);
						out.flush();
						out.close();
					} else if (msgPlusPortObject.msg.equals("search")) {
						Cursor cursor = null;
						String hashKey = null;

						if (!(msgPlusPortObject.key.equals("*") || msgPlusPortObject.key.equals("@"))) {
							try {
								hashKey = genHash(msgPlusPortObject.key);
							} catch (NoSuchAlgorithmException e) {
								Log.e(TAG, "CP query " + e.toString());
							}
							if (predecessor == null || (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessorHash) > 0)) {
								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
							} else if (predecessor == successor) {
								if (hashKey.compareTo(myPortHash) <= 0 && hashKey.compareTo(successorHash) < 0) {
									cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
								} else if (hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) < 0) {
									Log.d(TAG, "hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) < 0");
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
								Log.d(TAG, "else in server search");
								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//								cursor = actSynchronously("search", successor, msgPlusPortObject.key, msgPlusPortObject.originatorPort);
							}
							if (cursor.getCount() > 0) {
								cursor.moveToFirst();
								msgPlusPortObject.value = cursor.getString(1);
							}
						} else {
							cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);

							if (cursor.getCount() > 0) {
								cursor.moveToFirst();

								Map<String, String> cursorKeyValueMap = new HashMap<String, String>();
								while(!cursor.isAfterLast()) {
									cursorKeyValueMap.put(cursor.getString(0), cursor.getString(1));
									cursor.moveToNext();
								}
								msgPlusPortObject.keyValueMap = cursorKeyValueMap;
							}
						}

						ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
						out.writeObject(msgPlusPortObject);
						out.flush();
						out.close();
					} else if (msgPlusPortObject.msg.equals("replicate") || msgPlusPortObject.msg.equals("getMissedInsert")) {
//						Log.d(TAG, "replicate 1");
						Cursor cursor = null;
						if (msgPlusPortObject.msg.equals("replicate")) {
							cursor = query(CONTENT_URI, null, "insertion", new String[]{msgPlusPortObject.originatorPort}, null);
						} else if (msgPlusPortObject.msg.equals("getMissedInsert")) {
							cursor = query(CONTENT_URI, null, "replication",new String[]{msgPlusPortObject.originatorPort}, null);
						}

//						Log.d(TAG, "replicate 2");
						if (cursor.getCount() > 0) {
//							Log.d(TAG, "replicate 3");
							cursor.moveToFirst();

							Map<String, String> cursorKeyValueMap = new HashMap<String, String>();
							while(!cursor.isAfterLast()) {
								cursorKeyValueMap.put(cursor.getString(0), cursor.getString(1));
								cursor.moveToNext();
							}
							msgPlusPortObject.keyValueMap = cursorKeyValueMap;
						}

//						Log.d(TAG, "replicate 4");
						ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
						out.writeObject(msgPlusPortObject);
						out.flush();
						out.close();
					} else if (msgPlusPortObject.msg.equals("delete")) {
						Log.d(TAG, "ServerTask delete for key: " + msgPlusPortObject.key + " myPort: "+myPort);
						delete(CONTENT_URI, msgPlusPortObject.key, null);
						Log.d(TAG,
								"ServerTask delete DONE for key: " + msgPlusPortObject.key + " myPort: "+myPort);
					}

					clientSocket.close();
				} catch (Exception e){
					Log.e(TAG, "Server search Error: " + e.toString());
					e.printStackTrace();
				}
			}
		}

		protected void onProgressUpdate(String... strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */

			if (strings[0].equals("adjustRing")) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "adjustRing", strings[1], strings[2], strings[3]);
			} else if (strings[0].equals("connect")) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "connect", strings[1], strings[2]);
			} else if (strings[0].equals("search")) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "search", strings[1], strings[2], strings[3]);
			}

			return;
		}
	}

	public class SimpleDynamoDBHelper extends SQLiteOpenHelper {
		public static final int DATABASE_VERSION = 1;
//		private final String SQL_CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (" + COLUMN_NAME_KEY + " " + "TEXT," + COLUMN_NAME_VALUE + " TEXT)";
		private final String SQL_CREATE_TABLE =
		"CREATE TABLE " + TABLE_NAME + " (" + COLUMN_NAME_KEY + " " + "TEXT," + COLUMN_NAME_VALUE + " TEXT,type TEXT,port TEXT)";


		public SimpleDynamoDBHelper(Context context) {
//          Made in-memory database by not specifying 2nd arg name with DBName.
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
