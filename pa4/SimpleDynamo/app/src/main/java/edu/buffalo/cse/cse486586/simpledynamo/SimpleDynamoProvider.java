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
	boolean logging = true;
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

		if (logging == true) {
			Log.d(TAG, "delete key: " + selection);
		}
		String hashedKey = null;
		try {
			hashedKey = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			if (logging == true) {
				Log.e(TAG, "CP delete hashedKey originalKey: " + selection + " " + e.toString());
			}
			Log.e(TAG, "73 " + e.toString());
		}

		String[] selectionArgss = new String[]{selection};
		selection = COLUMN_NAME_KEY + "=?";

//		db.delete(TABLE_NAME, selection, selectionArgss);
		ContentValues newValues = new ContentValues();
		newValues.put("type", "deleted");
		db.update(TABLE_NAME, newValues, selection, selectionArgss);
		if (logging == true) {
			Log.d(TAG, "key deleted locally: " + selectionArgss[0]);
		}

		String portToStoreKey = getNodeToStoreKey(hashedKey);
		if (myPort.equals(portToStoreKey)) {

			String port = portToStoreKey;
			for (int i = 0; i < 2; i++) {
				port = successorMap.get(port);

				if (logging == true) {
					Log.d(TAG, "calling succ: " + port + " to delete from:" + myPort);
				}
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
		// INSERTION = true;
		String hashedKey = null;
		try {
			hashedKey = genHash(originalKey);
		} catch (NoSuchAlgorithmException e) {
			if (logging == true) {
				Log.e(TAG, "CP insert hashedKey " + e.toString());
			}
			Log.e(TAG, "122: "+ e.toString());
		}

		String portToStoreKey = getNodeToStoreKey(hashedKey);
		String succ1 = successorMap.get(portToStoreKey);
		String succ2 = successorMap.get(succ1);
//		String time = Long.toString(System.currentTimeMillis());

		 if (logging == true) {
		 	Log.d(TAG, "INSERTION " + myPort + " key: " + originalKey + " value:" +(String) values.get("value"));
		 }
		if (myPort.equals(portToStoreKey)) {
			// if (logging == true)Log.d(TAG, "INSERTION myPort.equals(portToStoreKey) " + myPort + " " + originalKey);
			ContentValues newValues = new ContentValues();

//			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
			newValues.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, (String) values.get("value"));
			newValues.put("type", "insertion");
			newValues.put("port", myPort);
//			newValues.put("time", time);

//			Cursor cursor = db.query(
//				TABLE_NAME,
//				null,
//				"key=?",
//				new String[]{originalKey},
//				null,
//				null,
//				"time DESC",
//				"1"
//			);
//
//			cursor.moveToFirst();
//			if (cursor.getCount() > 0) {
//				String time = (String) values.get("time");
//				if (msgType.equals("insertion") || time.compareTo(cursor.getString(4)) > 0) {
////				newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
//					db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY+"=?", new String[]{originalKey});
////				db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
//				} else if (msgType.equals("replication")) {
//					db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY+"=?", new String[]{originalKey});
//				}
////			if (cursor.getString(2).equals("insertion")) {
////				if (msgType.equals("replicate") || msgType.equals("getMissedInsert")) {
//////				if (msgType.equals("replicate") || cursor.getString(4).equals("getMissedInsert")) {
////					return uri;
////				}
////			}
//			} else {
//				newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
//				db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
//			}

//			int updated = db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY+"=?", new String[]{originalKey});
//			if (updated == 0) {
//				newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
//				db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
//			}

			Cursor cursor = db.query(
				TABLE_NAME,
				null,
				"key=?",
				new String[]{originalKey},
				null,
				null,
				"time DESC",
				"1"
			);

			cursor.moveToFirst();
			if (cursor.getCount() > 0) {
//				if (time != null && time.compareTo(cursor.getString(4)) > 0) {
				db.delete(TABLE_NAME, "key=?", new String[]{originalKey});
			}
			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
			String time = Long.toString(System.currentTimeMillis());
			newValues.put("time", time);
			db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);

//			db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_IGNORE);
//			db.insert(TABLE_NAME, null, values);

			// REPLICATION
			String port = myPort;
			for (int i=0; i<2; i++) {
				port = successorMap.get(port);

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
						"replication", port, originalKey,
						(String) values.get("value"), myPort, time);
//				actSynchronously("replication", port, originalKey, (String) values.get("value"), myPort);
			}

		} else if (myPort.equals(succ1) || myPort.equals(succ2)) {
			ContentValues newValues = new ContentValues();

//			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
			newValues.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, (String) values.get("value"));
			newValues.put("type", "replication");
			newValues.put("port", portToStoreKey);

			Cursor cursor = db.query(
					TABLE_NAME,
					null,
					"key=?",
					new String[]{originalKey},
					null,
					null,
					"time DESC",
					"1"
			);

			cursor.moveToFirst();
			if (cursor.getCount() > 0) {
//				if (time != null && time.compareTo(cursor.getString(4)) > 0) {
				db.delete(TABLE_NAME, "key=?", new String[]{originalKey});
			}
			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
			String time = Long.toString(System.currentTimeMillis());
			newValues.put("time", time);
			db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);

//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", portToStoreKey, originalKey, (String) values.get("value"), time);
			actSynchronously("insert", portToStoreKey, originalKey, (String) values.get("value"), time);

			if (myPort.equals(succ1)) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
						"replication", succ2, originalKey,
						(String) values.get("value"), portToStoreKey, time);
			} else {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
						"replication", succ1, originalKey,
						(String) values.get("value"), portToStoreKey, time);
			}
		} else {
			// if (logging == true)Log.d(TAG, "INSERTION myPort Not equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + originalKey);
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", portToStoreKey, originalKey, (String) values.get("value"));
			actSynchronously("insert", portToStoreKey, originalKey, (String) values.get("value"), Long.toString(System.currentTimeMillis()));
		}
		INSERTION = false;


		return uri;
	}

	public Uri insertReplication(Uri uri, String key, String value, String type, String port, String msgType, String time) {
		 if (logging == true) {
		 	Log.d(TAG,
				 "insertReplicationFunc: " + " key: "+ key + " value: "+ value + " type: "+ type + " port: "+ port + " myPort: " + myPort+" msgType: "+msgType);
		 }
		 try {
			 if (logging == true) {
			 	Log.d(TAG, "time:" + time);
			 }
		 } catch (Exception e) {
		 	if (logging == true) {
		 		Log.e(TAG, "time exception: "+e.toString());
		 	}
		 	Log.e(TAG, "280: "+e.toString());
		 }
		// INSERTION = true;
		ContentValues newValues = new ContentValues();

//		newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, originalKey);
		newValues.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, value);
		newValues.put("type", type);
		newValues.put("port", port);
		newValues.put("time", Long.toString(System.currentTimeMillis()));

		Cursor cursor = db.query(
			TABLE_NAME,
			null,
			"key=?",
			new String[]{key},
			null,
			null,
			"time DESC",
			"1"
		);

		cursor.moveToFirst();
		if (cursor.getCount() > 0) {
			if (msgType.equals("getMissedInsert")) {
				if (time != null && time.compareTo(cursor.getString(4)) > 0) {
					newValues.remove("time");
					newValues.put("time", time);
//					db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY + "=?", new String[]{key});
					db.delete(TABLE_NAME, "key=?", new String[]{key});
					newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
					db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);

//				} else {
//					newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
//					db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
				}
			} else if (msgType.equals("replication")) {
				if (time == null) {
					if (logging == true) {
						Log.d(TAG, "time is null in replication");
					}
					db.delete(TABLE_NAME, "key=?", new String[]{key});
					newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
					db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
				} else if (time.compareTo(cursor.getString(4)) > 0) {
//					db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY + "=?", new String[]{key});
					db.delete(TABLE_NAME, "key=?", new String[]{key});
					newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
					newValues.remove("time");
					newValues.put("time", time);
					db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
				}
			}
//			if (cursor.getString(2).equals("insertion")) {
//				if (msgType.equals("replicate") || msgType.equals("getMissedInsert")) {
////				if (msgType.equals("replicate") || cursor.getString(4).equals("getMissedInsert")) {
//					return uri;
//				}
//			}
		} else {
//			if (msgType.equals("getMissedInsert")) {
				newValues.remove("time");
				newValues.put("time", time);
//			}
			db.delete(TABLE_NAME, "key=?", new String[]{key});
			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
			db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
			if (logging == true) {
				Log.d(TAG, "timetime insertReplication " +
						"cursor is null: " + time);
			}
		}

//		int updated = db.update(TABLE_NAME, newValues, COLUMN_NAME_KEY+"=?", new String[]{key});
//		if (updated == 0) {
//			newValues.put(SimpleDynamoProvider.COLUMN_NAME_KEY, key);
//			db.insertWithOnConflict(TABLE_NAME, null, newValues, SQLiteDatabase.CONFLICT_REPLACE);
//		}
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

			if (logging == true) {
				Log.d(TAG, "CP onCreate: " + myPort);
			}

			myPortHash = portHashMap.get(myPort);

			successor = successorMap.get(myPort);
			successorHash = portHashMap.get(successor);

			predecessor = predecessorMap.get(myPort);
			predecessorHash = portHashMap.get(predecessor);


			db = DBHelper.getWritableDatabase();
			if (db != null) {

				db.delete(TABLE_NAME, null, null);

				String port = myPort;
				for (int i = 0; i < 2; i++) {
					port = predecessorMap.get(port);

					// if (logging == true)Log.d(TAG, "calling pred: " + port + " to replicate from:" + " " + myPort);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replicate", port, myPort);
//					actSynchronously("replicate", port, myPort, null);
				}

				port = myPort;
				for (int i = 0; i < 2; i++) {
					port = successorMap.get(port);
					// if (logging == true)Log.d(TAG, "calling succ: " + port + " to getMissedInsert from:" + myPort);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "getMissedInsert", port, myPort);
//					actSynchronously("getMissedInsert", port, myPort, null);
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
			if (logging == true) {
				Log.d(TAG, "score INSERTION BEING DONE FOR " +
						"KEY: " + selection);
			}
		}

		boolean ORIGIN = true;
		String originatorPort = myPort;
		if (selectionArgs!= null && selectionArgs.length >= 1 && selectionArgs[0] != null) {
			ORIGIN = false;
			originatorPort = selectionArgs[0];
		}
		if (logging == true) {
			Log.d(TAG, "qKEY selection:" + selection + " myPort: " + myPort + " " + "originatorPort: " + originatorPort);
		}

//		if (myPort.equals(originatorPort)) {
//			projection = new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE};
//		}

		sortOrder = "time DESC";

		if (logging == true) {
			Log.d(TAG, "replicate query insertion 1");
		}
		if (logging == true) {
			Log.d(TAG, "QUERY GLOBAL myPort: " + myPort + "," +
					" succ: "+ successor + ", pred: "+predecessor);
		}

		if (selection.equals("@") || selection.equals("LDump")) {
			String dupSelection;
			String[] selectionArgss = null;

			if (selection.equals("@")) {
				dupSelection = "type != ?";
				selectionArgss = new String[] {"deleted"};
			} else {
				dupSelection = null;
			}
			cursor = db.query(
				TABLE_NAME,
				projection,
				dupSelection,
				selectionArgss,
				null,
				null,
				sortOrder,
				null
			);
			if (logging == true) {
				Log.d(TAG, "query: " + selection);
			}
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
			if (logging == true) {
				Log.d(TAG,
						DatabaseUtils.dumpCursorToString(dummyCursor));
			}
			if (logging == true) {
				Log.d(TAG, "qKEY " + selection);
			}
		} else if (selection.equals("*") || selection.equals("GDump") || selection.equals("insertion") || selection.equals("replication")) {
			if (logging == true) {
				Log.d(TAG, "in * query myPort: " + myPort);
			}

			String dupSelection = selection;
			String[] selectionArgss = null;
			if (logging == true) {
				Log.d(TAG, "replicate query insertion 2");
			}
			if (selection.equals("insertion") || selection.equals("replication")) {
				if (logging == true) {
					Log.d(TAG, "replicate query insertion 3");
				}


				if (selection.equals("insertion")) {
					selectionArgss = new String[]{selection};
					selection = "type=?";
				} else if (selection.equals("replication")) {
					selectionArgss = new String[]{selection, originatorPort};
					selection = "type=? AND port=?";
				}

				if (logging == true) {
					Log.d(TAG, "replicate query insertion 4");
				}

				try {
					cursor = db.query(
							TABLE_NAME,
							null,
//							projection,
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
					if (logging == true) {
						Log.d(TAG, "replicate query " +
								"insertion 5");
					}
				} catch (Exception e) {
					if (logging == true) {
						Log.e(TAG,
								"Query ERROR " + selection + " " + selectionArgss[0]);
					}
					Log.e(TAG, "608: " + e.toString());
					// if (logging == true)Log.d(TAG, "Query ERROR " + selection + " " + selectionArgss[0] + " " + selectionArgss[1]);
				}
			} else {
				cursor = db.query(
					TABLE_NAME,
					projection,
					"type != ?",
					new String[] {"deleted"},
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
				if (logging == true) {
					Log.d(TAG, "replicate query insertion 6");
				}
				if (logging == true) {
					Log.d(TAG, "query " + dupSelection);
				}
				while(INSERTION) {
					if (logging == true) {
						Log.d(TAG,
								"score 2 "+dupSelection+" BEING DONE FOR KEY: " + selection);
					}
				}
//				if (dupSelection.equals("insertion") || dupSelection.equals(
//						"replication")) {
//					projection = null;
//				}
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
				if (logging == true) {
					Log.d(TAG,
							DatabaseUtils.dumpCursorToString(dummyCursor));
				}
//				cursor.moveToFirst();
				return cursor;
			}

//			if (successor != null && !successor.equals(originatorPort)) {
				if (logging == true) {
					Log.d(TAG, "successor != null && " +
							"!successor.equals" +
							"(originatorPort) " + myPort + " to succ "+ successor + " originatorPort: "+originatorPort);
				}
				String succ = successor;
				Cursor successorCursor = null;
				while (!succ.equals(myPort)) {
					try {

						successorCursor = actSynchronously("search", succ, originatorPort, "@", null);
						cursor = new MergeCursor(new Cursor[]{cursor, successorCursor});

						succ = successorMap.get(succ);
					} catch (Exception e) {
						if (logging == true) {
							Log.e(TAG, "successor != null && !successor.equals" +
									"(originatorPort) CATCH: " + e.toString());
						}
						Log.e(TAG, "696 " + e.toString());
						continue;
					}
				}
//			}
			if (logging == true) {
				Log.d(TAG, "query *");
			}
			dummyCursor = cursor;
			if (logging == true) {
				Log.d(TAG,
						DatabaseUtils.dumpCursorToString(dummyCursor));
			}
		} else {
			String hashedKey = null;
			try {
				hashedKey = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				if (logging == true) {
					Log.e(TAG, "CP insert hashedKey " + e.toString());
				}
				Log.e(TAG, "641" + e.toString());
			}

			String[] selectionArgss = new String[]{selection, "deleted"};
			String portToStoreKey = getNodeToStoreKey(hashedKey);

			if (logging == true) {
				Log.d(TAG,
						"QUERYING " + myPort + " " + selection);
			}
			if (logging == true) {
				Log.d(TAG, "QUERYING myPort.equals" +
						"(portToStoreKey) " + myPort + " " + selection);
			}

			selection = COLUMN_NAME_KEY + "=? AND type != ?";

			while(INSERTION) {
				if (logging == true) {
					Log.d(TAG,
							"score 3 "+selection+" BEING DONE FOR KEY: " + selectionArgss[0] + " type: " + selectionArgss[1]);
				}
			}


			cursor = db.query(
					TABLE_NAME,
					null,
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

				if (logging == true) {
					Log.d(TAG, "qKEY " + selectionArgss[0]);
				}
			try {
//				if (cursor.getCount() > 0 && myPort.equals(portToStoreKey)) {
//				if (myPort.equals(portToStoreKey)) {
//					while ((cursor != null && cursor.getCount() <= 0) || cursor == null) {
				while(INSERTION) {
					if (logging == true) {
						Log.d(TAG, "score 3 "+selection+" " +
								"BEING DONE FOR KEY: " + selectionArgss[0] + " type: " + selectionArgss[1]);
					}
				}
						cursor = db.query(
								TABLE_NAME,
								null,
								selection,
								selectionArgss,
								null,
								null,
								sortOrder,
								"1"
						);
//					}

//					String port = portToStoreKey;
//					Cursor cursor1 = null;
//					Cursor cursor2 = null;
//
//
//
//					try {
//						if (logging == true)Log.d(TAG, "cursor 1");
//						port = successorMap.get(port);
//						if (!port.equals(originatorPort)) {
//							cursor1 = actSynchronously("search", port, originatorPort, selectionArgss[0]);
//						}
//					} catch (Exception e) {
//						if (logging == true)Log.d(TAG, "cursor1 is null maybe");
//						Log.e(TAG, "query myPort.equals(portToStoreKey) first" +
//								" succ: " + port + " key: "+selectionArgss[0] + " originatorPort:"+originatorPort +" err: "+ e.toString());
//					}
//
//					try {
//						if (logging == true)Log.d(TAG, "cursor 2");
//						port = successorMap.get(port);
//						if (!port.equals(originatorPort)) {
//							cursor2 = actSynchronously("search", port, originatorPort, selectionArgss[0]);
//						}
//					} catch (Exception e) {
//						if (logging == true)Log.d(TAG, "cursor2 is null maybe");
//						Log.e(TAG, "query myPort.equals(portToStoreKey) " +
//								"second" +
//								" succ: " + port + " key: "+selectionArgss[0] + " originatorPort:"+originatorPort +" err: "+ e.toString());
//					}
//
//					String time = null;
//					String time1 = null;
//					String time2 = null;
//
//
//
//					if (cursor != null && cursor.getCount() > 0) {
//						dummyCursor = cursor;
//						if (logging == true)Log.d(TAG, "dumping time cursor: " + DatabaseUtils.dumpCursorToString(dummyCursor));
//
//						cursor.moveToFirst();
//						time = cursor.getString(4);
//
////						MatrixCursor matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});
////						MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
////						newRow.add(cursor.getString(0));
////						newRow.add(cursor.getString(1));
////
////						cursor = matrixCursor;
//					}
//
//					try {
//						if (cursor1 != null && cursor1.getCount() > 0) {
//							if (logging == true)Log.d(TAG, "cursor1 != null && cursor1.getCount() > 0");
//							cursor1.moveToFirst();
//							time1 = cursor1.getString(4);
//						}
//					} catch (Exception e) {
//						Log.e(TAG, "cursor1 is broken: "+e.toString());
//					}
//
//					try {
//						if (cursor2 != null && cursor2.getCount() > 0) {
//							if (logging == true)Log.d(TAG, "cursor2 != null && cursor2.getCount() > 0");
//							cursor2.moveToFirst();
//							time2 = cursor2.getString(4);
//						}
//					} catch (Exception e) {
//						Log.e(TAG, "cursor2 is broken: "+e.toString());
//					}
//
//					if (time != null) {
//						if (time1 != null && time1.compareTo(time) > 0) {
//							if (logging == true)Log.d(TAG, "time1 != null && time1.compareTo(time) > 0");
//							cursor = cursor1;
//							time = time1;
//						}
//
//						if (time2 != null && time2.compareTo(time) > 0) {
//							if (logging == true)Log.d(TAG, "time2 != null && time2.compareTo(time) > 0");
//							cursor = cursor2;
//							time = time2;
//						}
//					} else {
//						if (time1 != null && time2 != null && time1.compareTo(time2) > 0) {
//							if (logging == true)Log.d(TAG, "time1 != null && time2 != null && time1.compareTo(time2) > 0");
//							cursor = cursor1;
//						} else if (time1 != null) {
//							if (logging == true)Log.d(TAG, "time1 != null");
//							cursor = cursor1;
//						} else {
//							if (logging == true)Log.d(TAG, "time1 == null && time2 == null");
//							cursor = cursor2;
//						}
//					}
//
////					if (cursor != null && cursor.getCount() > 0) {
////						if (cursor1 != null && cursor1.getCount() > 0) {
////							String time = cursor.getString(4);
////							String time1 = cursor1.getString(4);
////
////							if (time.compareTo(time1) < 0) {
////								cursor = cursor1;
////							}
////						}
////
////						if (cursor2 != null && cursor2.getCount() > 0) {
////							String time = cursor.getString(4);
////							String time2 = cursor2.getString(4);
////
////							if (time.compareTo(time2) < 0) {
////								cursor = cursor2;
////							}
////						}
////					} else {
////						if (cursor1 != null && cursor1.getCount() > 0) {
////							String time = cursor.getString(4);
////							String time1 = cursor1.getString(4);
////
////							if (time.compareTo(time1) < 0) {
////								cursor = cursor1;
////							}
////						}
////					}
//
//					dummyCursor = cursor;
//					if (logging == true)Log.d(TAG, "query cursor myPort.equals (portToStoreKey) ");
//					if (logging == true)Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
//					cursor.moveToFirst();
//					if (logging == true)Log.d(TAG,"cursor qKEY " + cursor.getString(0));
//					if (logging == true)Log.d(TAG,"cursor qVALUE "+ cursor.getString(1));
//				} else {
//					if (!myPort.equals(portToStoreKey) && !portToStoreKey.equals(originatorPort)) {
//						// TODO
//						//HERERERERERERERERRERERER CHECK IF KEY IS REPLICATED LOCALLY
//						// OR JUST QUERY LOCAL, IF NOT FOUND THEN SEARCH THE RING
//						if (logging == true)Log.d(TAG, "QUERYING myPort Not equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + selection);
//						while (((cursor != null && cursor.getCount() <= 0) || cursor == null) && !originatorPort.equals(portToStoreKey)) {
					// TODO
					//HERERERERERERERERRERERER CHECK IF KEY IS REPLICATED LOCALLY
					// OR JUST QUERY LOCAL, IF NOT FOUND THEN SEARCH THE RING









					//	GET ALL VALUES AND RETURN THE VALUE WITH MOST RECENT TIMESTAMP

//					if (cursor.getCount() > 0) {
//
//					}
//
//					String port = portToStoreKey;
//					for(int i=0; i<3; i++) {
//						if (port.equals(originatorPort) || ) {
//							continue;
//						}
//
//
//					}











					if (logging == true) {
						Log.d(TAG, "QUERYING myPort Not " +
								"equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + selection);
					}


					String port = portToStoreKey;
					Cursor cursor1 = null;
					Cursor cursor2 = null;
					Cursor cursor3 = null;





					try {
						if (logging == true) {
							Log.d(TAG, "cursor 2");
						}
						port = successorMap.get(port);
						if (!port.equals(originatorPort) && !port.equals(myPort) && myPort.equals(originatorPort)) {
							cursor2 = actSynchronously("search", port, originatorPort, selectionArgss[0], null);
						}
					} catch (Exception e) {
						if (logging == true) {
							Log.d(TAG, "cursor2 is null " +
									"maybe");
						}
						if (logging == true) {
							Log.e(TAG, "query myPort.equals(portToStoreKey) " +
									"second" +
									" succ: " + port + " key: "+selectionArgss[0] + " originatorPort:"+originatorPort +" err: "+ e.toString());
						}
						Log.e(TAG, "998 " + e.toString());
					}

					try {
						if (logging == true) {
							Log.d(TAG, "cursor 3");
						}
						port = successorMap.get(port);
						if (!port.equals(originatorPort) && !port.equals(myPort) && myPort.equals(originatorPort)) {
							cursor3 = actSynchronously("search", port, originatorPort, selectionArgss[0], null);
						}
					} catch (Exception e) {
						if (logging == true) {
							Log.d(TAG, "cursor3 is null " +
									"maybe");
						}
						if (logging == true) {
							Log.e(TAG, "query myPort.equals(portToStoreKey) " +
									"third" +
									" succ: " + port + " key: "+selectionArgss[0] + " originatorPort:"+originatorPort +" err: "+ e.toString());
						}
						Log.e(TAG, "912 " + e.toString());
					}

				try {
					if (logging == true) {
						Log.d(TAG, "cursor 1");
					}
					if (!portToStoreKey.equals(originatorPort) && !portToStoreKey.equals(myPort) && myPort.equals(originatorPort)) {
						cursor1 = actSynchronously("search", portToStoreKey, originatorPort, selectionArgss[0], null);
					}
				} catch (Exception e) {
					if (logging == true) {
						Log.d(TAG, "cursor1 is null maybe");
					}
					if (logging == true) {
						Log.e(TAG, "query myPort.equals(portToStoreKey) first" +
								" succ: " + port + " key: "+selectionArgss[0] + " originatorPort:"+originatorPort +" err: "+ e.toString());
					}
					Log.e(TAG, "924 " + e.toString());
				}

					String time = null;
					String time1 = null;
					String time2 = null;
					String time3 = null;



					if (cursor != null && cursor.getCount() > 0) {
						dummyCursor = cursor;
						if (logging == true) {
							Log.d(TAG, "dumping time cursor:" +
									" " + DatabaseUtils.dumpCursorToString(dummyCursor));
						}

						cursor.moveToFirst();
						time = cursor.getString(4);

//						MatrixCursor matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});
//						MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
//						newRow.add(cursor.getString(0));
//						newRow.add(cursor.getString(1));
//
//						cursor = matrixCursor;
					}

					try {
						if (cursor1 != null && cursor1.getCount() > 0) {
							if (logging == true) {
								Log.d(TAG, "cursor1 != null " +
										"&& cursor1.getCount() > 0");
							}
							cursor1.moveToFirst();
							time1 = cursor1.getString(4);
							if (logging == true) {
								Log.d(TAG, "dumping cursor1:" +
										" " + DatabaseUtils.dumpCursorToString(cursor1));
							}
						}
					} catch (Exception e) {
						if (logging == true) {
							Log.e(TAG, "cursor1 is broken: "+e.toString());
						}
						Log.e(TAG, "958 " + e.toString());
					}

					try {
						if (cursor2 != null && cursor2.getCount() > 0) {
							if (logging == true) {
								Log.d(TAG, "cursor2 != null " +
										"&& cursor2.getCount() > 0");
							}
							cursor2.moveToFirst();
							time2 = cursor2.getString(4);
							if (logging == true) {
								Log.d(TAG, "dumping cursor2:" +
										" " + DatabaseUtils.dumpCursorToString(cursor2));
							}
						}
					} catch (Exception e) {
						if (logging == true) {
							Log.e(TAG, "cursor2 is broken: "+e.toString());
						}
						Log.e(TAG, "970 " + e.toString());
					}

					try {
						if (cursor3 != null && cursor3.getCount() > 0) {
							if (logging == true) {
								Log.d(TAG, "cursor3 != null " +
										"&& cursor3.getCount()" +
										" > 0");
							}
							cursor3.moveToFirst();
							time3 = cursor3.getString(4);
							if (logging == true) {
								Log.d(TAG, "dumping cursor3:" +
										" " + DatabaseUtils.dumpCursorToString(cursor3));
							}
						}
					} catch (Exception e) {
						if (logging == true) {
							Log.e(TAG, "cursor3 is broken: "+e.toString());
						}
						Log.e(TAG, "983 " + e.toString());
					}


					if (time != null) {
						if (logging == true) {
							Log.d(TAG, "time != null");
						}
						if (time1 != null && time1.compareTo(time) > 0) {
							if (logging == true) {
								Log.d(TAG, "time1 != null &&" +
										" time1.compareTo(time) > 0");
							}
							cursor = cursor1;
							time = time1;
						}

						if (time2 != null && time2.compareTo(time) > 0) {
							if (logging == true) {
								Log.d(TAG, "time2 != null &&" +
										" time2.compareTo(time) > 0");
							}
							cursor = cursor2;
							time = time2;
						}

						if (time3 != null && time3.compareTo(time) > 0) {
							if (logging == true) {
								Log.d(TAG, "time3 != null &&" +
										" time3.compareTo" +
										"(time) > 0");
							}
							cursor = cursor3;
							time = time3;
						}
					} else {
						if (logging == true) {
							Log.d(TAG, "time == null");
						}
						if (time1 != null) {
							if (logging == true) {
								Log.d(TAG, "time1 != null");
							}
							cursor = cursor1;

							if (time2 != null && time2.compareTo(time1) > 0) {
								if (logging == true) {
									Log.d(TAG, "time2 != " +
											"null && time2.compareTo(time1) > 0");
								}
								cursor = cursor2;
								time1 = time2;
							}

							if (time3 != null && time3.compareTo(time1) > 0) {
								if (logging == true) {
									Log.d(TAG, "time3 != " +
											"null && time3.compareTo(time1) > 0");
								}
								cursor = cursor3;
							}
						} else {
							if (time2 != null) {
								if (logging == true) {
									Log.d(TAG, "time2 != " +
											"null");
								}
								cursor = cursor2;

								if (time3 != null && time3.compareTo(time2) > 0) {
									if (logging == true) {
										Log.d(TAG, "time3 !=" +
												" null && time3.compareTo(time2) > 0");
									}
									cursor = cursor3;
								}
							} else {
								if (time3 != null) {
									if (logging == true) {
										Log.d(TAG, "time3 !=" +
												" null");
									}
									cursor = cursor3;
								}
							}
						}
					}
















//					while ((cursor != null && cursor.getCount() <= 0) || cursor == null) {
//						String port = portToStoreKey;
//
////						Cursor cursor2 = null;
////						Cursor cursor3 = null;
//
//						while ((cursor != null && cursor.getCount() <= 0) || cursor == null) {
////					while (cursor == null || cursor.getCount() <= 0) {
//							try {
//								if (logging == true)Log.d(TAG, "query cursor myPort Not equals (portToStoreKey) ");
//								if (!port.equals(originatorPort)) {
//									cursor = actSynchronously("search", port, originatorPort, selectionArgss[0]);
//								}
//
//								dummyCursor = cursor;
//								if (logging == true)Log.d(TAG,
//										"dumpcursor 817: "+DatabaseUtils.dumpCursorToString(dummyCursor));
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
////							while (cursor.getCount() <= 0) {
////								portToStoreKey = successorMap.get(portToStoreKey);
////								cursor = actSynchronously("search", portToStoreKey, originatorPort, selectionArgss[0]);
////							}
////					cursor.moveToFirst();
//							} catch (Exception e) {
//								Log.e(TAG, "first call to actSynchronously: " + myPort);
//								e.printStackTrace();
////							portToStoreKey = successorMap.get(portToStoreKey);
////							continue;
//							}
//							port = successorMap.get(port);
//						}
//					}
//				}
			} catch (Exception e) {
				if (logging == true) {
					Log.e(TAG, "querying else " + e.toString());
				}
				Log.e(TAG, "1116 " + e.toString());
				e.printStackTrace();
			}

//			=================================
//			if (myPort.equals(portToStoreKey)) {
////				if (logging == true)Log.d(TAG, "QUERYING myPort.equals(portToStoreKey) " + myPort + " " + selection);
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
////				if (logging == true)Log.d(TAG, "qKEY " + selectionArgss[0]);
//				try {
//					if (cursor.getCount() > 0) {
//						if (logging == true)Log.d(TAG, "query cursor myPort.equals" +
//								"(portToStoreKey) ");
//						if (logging == true)Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
////						cursor.moveToFirst();
////						if (logging == true)Log.d(TAG,"cursor qKEY " + cursor.getString(0));
////						if (logging == true)Log.d(TAG,"cursor qVALUE "+ cursor.getString(1));
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
//				if (logging == true)Log.d(TAG, "QUERYING myPort Not equals(portToStoreKey) " + myPort + " " + portToStoreKey + " " + selection);
//				try {
//					cursor = actSynchronously("search", portToStoreKey, originatorPort, selectionArgss[0]);
//					if (logging == true)Log.d(TAG, "query cursor myPort Not equals" +
//							"(portToStoreKey) ");
//					dummyCursor = cursor;
//					if (logging == true)Log.d(TAG, DatabaseUtils.dumpCursorToString(dummyCursor));
////					cursor.moveToFirst();
//				} catch (Exception e) {
//					Log.e(TAG, "first call to actSynchronously: " + myPort);
//					e.printStackTrace();
//				}
//			}
		}

//		if (!selection.equals("LDump") && myPort.equals(originatorPort) && ORIGIN == true) {
		if (!selection.equals("LDump") && ORIGIN == true) {
			MatrixCursor matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});

			cursor.moveToFirst();
			while(!cursor.isAfterLast()) {
				MatrixCursor.RowBuilder newRow = matrixCursor.newRow();

//				cursor.moveToFirst();
				newRow.add(cursor.getString(0));
				newRow.add(cursor.getString(1));

				cursor.moveToNext();
			}

			cursor = matrixCursor;
		}

		return cursor;
	}

	public Cursor actSynchronously(String msgType, String portToConnect,
								   String originatorPort, String key,
								   String time) {
		if (msgType.equals("search")) {
			try {
				if (logging == true) {
					Log.d(TAG, "actSynchronously Start for " +
							"msgType:" + msgType +
							" " + "portToConnect: " + portToConnect + " key: " + key + " originatorPort:" + originatorPort + " time: "+time);
				}
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));
				socket.setSoTimeout(500);


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
//				matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE});
				matrixCursor = new MatrixCursor(new String[]{COLUMN_NAME_KEY, COLUMN_NAME_VALUE, "type", "port", "time"});

				if (!(key.equals("*") || key.equals("@"))) {
					MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
					if (!ack.value.equals(null)) {
						newRow.add(ack.key);
//						newRow.add(ack.value);

						String val = ack.value;

						String[] value_type_port_time = val.split("-");
//							newRow.add(entry.getValue());
						newRow.add(value_type_port_time[0]);
						newRow.add(value_type_port_time[1]);
						newRow.add(value_type_port_time[2]);
						newRow.add(value_type_port_time[3]);

						return matrixCursor;
					}
				} else {
					Map<String, String> cursorKeyValueMap = ack.keyValueMap;

					if (cursorKeyValueMap != null && !cursorKeyValueMap.isEmpty()) {
						for (Map.Entry<String, String> entry : cursorKeyValueMap.entrySet()) {
							MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
							newRow.add(entry.getKey());

							String val = entry.getValue();

							String[] value_type_port_time = val.split("-");
//							newRow.add(entry.getValue());
							newRow.add(value_type_port_time[0]);
							newRow.add(value_type_port_time[1]);
							newRow.add(value_type_port_time[2]);
							newRow.add(value_type_port_time[3]);
						}
					}

					return matrixCursor;
				}

				in.close();
				socket.close();
			} catch (Exception e) {
				if (logging == true) {
					Log.e(TAG,
							"query Client search ExceptionFinal: " + e.toString() +
									" actSynchronously Start for msgType:" + msgType + " " + "portToConnect: " + portToConnect + " key: " + key + " originatorPort:" + originatorPort);
				}
				Log.e(TAG, "1280 " + e.toString());
				e.printStackTrace();
			}

		} else if (msgType.equals("insert") || msgType.equals("replication")) {
			messageStruct ack = new messageStruct();

//			String msgType = msgs[0];
			String nxtSuccessor = portToConnect;
			String _key = originatorPort;
			String value = key;

			if (logging == true) {
				Log.d(TAG,
						msgType + " ClienTask for key: " + _key + " myPort: " + myPort + " sent to: " + nxtSuccessor +
								" ASYNC MSG 1");
			}
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
				socket.setSoTimeout(500);

				if (logging == true) {
					Log.d(TAG, msgType + " ClienTask for " +
							"key: " + _key + " " +
							"myPort: " + myPort + " sent to: " + nxtSuccessor +
							" ASYNC MSG 2");
				}
				messageStruct msgStruct;
//				if (msgType.equals("replication")) {
//					msgStruct = new messageStruct(
//							msgType,
//							msgs[4],
//							_key,
//							value
//					);
//				} else {
					msgStruct = new messageStruct(
							msgType,
							null,
							_key,
							value,
							time,
							null
					);
//				}

				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgStruct);
				out.flush();

				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				ack = (messageStruct) in.readObject();
				in.close();

				socket.close();
//					if (logging == true)Log.d(TAG, msgType + " ClienTask for key: " + _key + " "
//					+ "myPort: " + myPort + " sent to: " + nxtSuccessor + " DUPLICATE MSG SENT");
			} catch (EOFException e) {
				if (logging == true) {
					Log.e(TAG, "ClientTask " + msgType + " " + nxtSuccessor + " " +
							"SocketTimeoutException " + _key + " : " + e.toString());
				}
				Log.e(TAG, "1335 " + e.toString());

				String[] forClientPublishProgress = null;
				if (msgType.equals("insert")) {
					String port = nxtSuccessor;

					for (int i = 0; i < 2; i++) {
						port = successorMap.get(port);

//						forClientPublishProgress = new String[]{
//								"replication",
//								port,
//								_key,
//								value,
//								nxtSuccessor
//						};
//
//						publishProgress(forClientPublishProgress);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replication", port, _key, value, nxtSuccessor, time);
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
//								_key,
//								value,
//								nxtSuccessor
//							};
//						}
				}
			} catch (Exception e) {
				if (logging == true) {
					Log.e(TAG,
							"Client " + msgType + " ExceptionFinal: " + nxtSuccessor + " " + e.toString());
				}
				Log.e(TAG, "1374 " + e.toString());
				e.printStackTrace();
			}
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

				if (logging == true) {
					Log.d(TAG, msgType + " ClienTask for " +
							"key: " + key + " myPort: " + myPort + " sent to: " + nxtSuccessor);
				}
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nxtSuccessor));
					socket.setSoTimeout(500);

					if (logging == true) {
						Log.d(TAG, msgType + " ClienTask for" +
								" key: " + key + " " + "myPort: " + myPort + " sent to: " + nxtSuccessor + " DUPLICATE MSG");
					}
					messageStruct msgStruct;
					// TIME CAN CREATE PROBLEM HERE MAYBE
					if (msgType.equals("replication")) {
						msgStruct = new messageStruct(
							msgType,
							msgs[4],
							key,
							value,
							msgs[5],
							null
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
//					if (logging == true)Log.d(TAG, msgType + " ClienTask for key: " + key + " " + "myPort: " + myPort + " sent to: " + nxtSuccessor + " DUPLICATE MSG SENT");
				} catch (EOFException e) {
					if (logging == true) {
						Log.e(TAG,
								"ClientTask ASYNC " + msgType + " " + nxtSuccessor + " SocketTimeoutException " + key + " : " + e.toString());
					}
					Log.e(TAG, "1662 " + e.toString());

					String[] forClientPublishProgress = null;
					if (msgType.equals("insert")) {
						String port = nxtSuccessor;

//						for (int i=0; i<2; i++) {
//							port = successorMap.get(port);
//
//							if (logging == true)Log.d(TAG, "insert failed for Async client: " + nxtSuccessor + " key: " + key + " value: "+value);
//							forClientPublishProgress = new String[]{
//								"replication",
//								port,
//								key,
//								value,
//								nxtSuccessor,
//								null
//							};
//
//							publishProgress(forClientPublishProgress);
//						}
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
					if (logging == true) {
						Log.e(TAG,
								"Client " + msgType + " ExceptionFinal: " + nxtSuccessor + " " + e.toString());
					}
					Log.e(TAG, "1506 " + e.toString());
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
					if (logging == true) {
						Log.e(TAG, "Client delete ExceptionFinal: " + e.toString());
					}
					Log.e(TAG, "1530 " + e.toString());
					e.printStackTrace();
				}
			} else if (msgType.equals("replicate") || msgType.equals("getMissedInsert")) {
				String originatorPort = msgs[2];

//				if (logging == true)Log.d(TAG, "Insert ClienTask for key: " + key + " myPort: " + myPort + " sent to: " + nxtSuccessor);
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
							if (logging == true) {
								Log.d(TAG,
										"looping "+msgType+" for failed nodes " + entry.getKey() + " myPort: " + myPort + " from node: " + nxtSuccessor);
							}
//							ContentValues values = new ContentValues();

//							values.put(SimpleDynamoProvider.COLUMN_NAME_KEY, entry.getKey());
//							values.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, entry.getValue());

							String values_type = null;
							String values_port = null;
							if (msgType.equals("getMissedInsert")) {
								values_type = "insertion";
								values_port = myPort;
							} else if (msgType.equals("replicate")) {
								values_type = "replication";
								values_port = nxtSuccessor;
							}

							String k, v;
							k = entry.getKey();
							v = entry.getValue();

							String[] value_time = v.split("-");
							if (logging == true) {
								Log.d(TAG,
										msgType+" printing | k: "+k+" v: "+value_time[0] + " t: "+value_time[1] + " oV:"+v);
							}

							insertReplication(CONTENT_URI, k,
									value_time[0], values_type,
									values_port, msgType, value_time[1]);
						}
					}

					in.close();

					socket.close();
				} catch (Exception e) {
					if (logging == true) {
						Log.e(TAG, "Client " + msgType + " ExceptionFinal: " + e.toString());
					}
					Log.e(TAG, "1594 " + e.toString());
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
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
						"replication", strings[1], strings[2], strings[3],
						strings[4], strings[5]);
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
//				if (logging == true)Log.d(TAG, "GLOBAL myPort: " + myPort + "," + " succ: "+ successor + ", pred: "+predecessor);
				try {
					Socket clientSocket = null;
					clientSocket = serverSocket.accept();

					ObjectInputStream in = null;
					in = new ObjectInputStream(clientSocket.getInputStream());

					msgPlusPortObject = (messageStruct) in.readObject();

					if (msgPlusPortObject.msg.equals("insert") || msgPlusPortObject.msg.equals("replication")) {
//						ContentValues values = new ContentValues();
//
//						values.put(SimpleDynamoProvider.COLUMN_NAME_KEY, msgPlusPortObject.key);
//						values.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, msgPlusPortObject.value);

						if (msgPlusPortObject.msg.equals("replication")) {
							if (logging == true) {
								Log.d(TAG,
										"insertReplication ServerTask for key:" + " " + msgPlusPortObject.key + " myPort: " + myPort);
							}

							String values_type = "replication";
							String values_port = msgPlusPortObject.originatorPort;
							insertReplication(CONTENT_URI,
									msgPlusPortObject.key,
									msgPlusPortObject.value, values_type,
									values_port, msgPlusPortObject.msg,
									msgPlusPortObject.time);
						} else {
							if (logging == true) {
								Log.d(TAG, "Insert " +
										"ServerTask for key: " + msgPlusPortObject.key + " myPort: " + myPort);
							}
							ContentValues values = new ContentValues();

							values.put(SimpleDynamoProvider.COLUMN_NAME_KEY, msgPlusPortObject.key);
							values.put(SimpleDynamoProvider.COLUMN_NAME_VALUE, msgPlusPortObject.value);
							values.put("type", "insertion");
							values.put("port", myPort);
							insert(CONTENT_URI, values);
//							if (logging == true)Log.d(TAG, "Insert DONE ServerTask for key: " + msgPlusPortObject.key + " myPort: " + myPort);
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
								if (logging == true) {
									Log.e(TAG, "CP query " + e.toString());
								}
								Log.e(TAG, "1679 " + e.toString());
							}
//							if (predecessor == null || (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessorHash) > 0)) {
//								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//							} else if (predecessor == successor) {
//								if (hashKey.compareTo(myPortHash) <= 0 && hashKey.compareTo(successorHash) < 0) {
//									cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//								} else if (hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) < 0) {
//									if (logging == true)Log.d(TAG, "hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) < 0");
//									cursor = actSynchronously("search", successor, msgPlusPortObject.originatorPort, msgPlusPortObject.key);
//								} else if (hashKey.compareTo(myPortHash) > 0 && hashKey.compareTo(successorHash) > 0) {
//									cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//								}
//							} else if (myPortHash.compareTo(hashKey) >= 0 && hashKey.compareTo(predecessorHash) > 0) {
//								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//							} else if (myPortHash.compareTo(hashKey) < 0 && predecessorHash.compareTo(hashKey) < 0 && successorHash.compareTo(hashKey) < 0 && myPortHash.compareTo(predecessorHash) < 0) {
//								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//							} else if (myPortHash.compareTo(hashKey) > 0 && predecessorHash.compareTo(hashKey) > 0 && successorHash.compareTo(hashKey) > 0 && myPortHash.compareTo(predecessorHash) < 0) {
//								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//							} else {
								if (logging == true) {
									Log.d(TAG, "else in " +
											"server search");
								}
								cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);
//								cursor = actSynchronously("search", successor, msgPlusPortObject.originatorPort, msgPlusPortObject.key);
//							}
							if (cursor.getCount() > 0) {
								cursor.moveToFirst();
								String val = cursor.getString(1) + "-" + cursor.getString(2) + "-" + cursor.getString(3) + "-" + cursor.getString(4);
//								msgPlusPortObject.value = cursor.getString(1);
								msgPlusPortObject.value = val;
							}
						} else {
							cursor = query(CONTENT_URI, null, msgPlusPortObject.key, new String[]{msgPlusPortObject.originatorPort}, null);

							if (cursor.getCount() > 0) {
								cursor.moveToFirst();

								Map<String, String> cursorKeyValueMap = new HashMap<String, String>();
								while(!cursor.isAfterLast()) {
									String val = cursor.getString(1) + "-" + cursor.getString(2) + "-" + cursor.getString(3) + "-" + cursor.getString(4);
									cursorKeyValueMap.put(cursor.getString(0), val);
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
//						if (logging == true)Log.d(TAG, "replicate 1");
						Cursor cursor = null;
						if (msgPlusPortObject.msg.equals("replicate")) {
							cursor = query(CONTENT_URI, null, "insertion", new String[]{msgPlusPortObject.originatorPort}, null);
						} else if (msgPlusPortObject.msg.equals("getMissedInsert")) {
							cursor = query(CONTENT_URI, null, "replication",new String[]{msgPlusPortObject.originatorPort}, null);
						}

//						if (logging == true)Log.d(TAG, "replicate 2");
						if (cursor.getCount() > 0) {
//							if (logging == true)Log.d(TAG, "replicate 3");
							cursor.moveToFirst();


							String k, v;
							Map<String, String> cursorKeyValueMap = new HashMap<String, String>();
							while(!cursor.isAfterLast()) {
								k = cursor.getString(0);
								v = cursor.getString(1)+"-"+cursor.getString(4);
								if (logging == true) {
									Log.d(TAG,
											msgPlusPortObject.msg + " printing | " + "k: "+k+" v: "+v);
								}
								cursorKeyValueMap.put(k, v);
								cursor.moveToNext();
							}
							msgPlusPortObject.keyValueMap = cursorKeyValueMap;
						}

//						if (logging == true)Log.d(TAG, "replicate 4");
						ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
						out.writeObject(msgPlusPortObject);
						out.flush();
						out.close();
					} else if (msgPlusPortObject.msg.equals("delete")) {
						if (logging == true) {
							Log.d(TAG, "ServerTask delete " +
									"for key: " + msgPlusPortObject.key + " myPort: "+myPort);
						}
						delete(CONTENT_URI, msgPlusPortObject.key, null);
						if (logging == true) {
							Log.d(TAG,
									"ServerTask delete DONE for key: " + msgPlusPortObject.key + " myPort: "+myPort);
						}
					}

					clientSocket.close();
				} catch (Exception e){
					if (logging == true) {
						Log.e(TAG, "Server search Error: " + e.toString());
					}
					Log.e(TAG, "1771 " + e.toString());
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
		"CREATE TABLE " + TABLE_NAME + " (" + COLUMN_NAME_KEY + " " + "TEXT," + COLUMN_NAME_VALUE + " TEXT,type TEXT,port TEXT,time TEXT)";


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
	String msg, port, key, value, successor, predecessor, originatorPort,
	adjustNode, adjustNodeToPort, time;
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

	public messageStruct(String message, String _originatorPort, String _key,
						 String _value, String _time, String dummy) {
		msg = message;
		originatorPort = _originatorPort;
		key = _key;
		value = _value;
		time = _time;
	}
}
