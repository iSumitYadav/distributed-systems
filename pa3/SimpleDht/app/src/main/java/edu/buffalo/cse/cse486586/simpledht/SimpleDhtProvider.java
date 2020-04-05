package edu.buffalo.cse.cse486586.simpledht;

import java.util.Formatter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


import android.net.Uri;
import android.util.Log;

import android.content.Context;
import android.content.ContentValues;
import android.content.ContentProvider;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;


public class SimpleDhtProvider extends ContentProvider {
    static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");

    private SQLiteDatabase db;
    public static final String TABLE_NAME = "messages";
    public static final String COLUMN_NAME_KEY = "key";
    public static final String COLUMN_NAME_VALUE = "value";

    static final String TAG = SimpleDhtActivity.class.getSimpleName();

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

        db.insert(TABLE_NAME, null, values);

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        SimpleDHTDBHelper DBHelper = new SimpleDHTDBHelper(getContext());

        db = DBHelper.getWritableDatabase();
        if(db != null){
            return true;
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        Cursor cursor;

        if(selection.equals("*") || selection.equals("@")){
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
        }else{
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
            cursor.moveToFirst();
            Log.d("qVALUE", cursor.getString(1));
        }

        return cursor;
    }

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
}
