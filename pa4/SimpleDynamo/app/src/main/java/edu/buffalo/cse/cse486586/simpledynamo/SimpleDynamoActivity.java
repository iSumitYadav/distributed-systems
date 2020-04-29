package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;
import android.os.Bundle;
import android.view.Menu;
import android.view.View;
import android.app.Activity;
import android.widget.Button;
import android.widget.TextView;
import android.database.Cursor;
import android.text.method.ScrollingMovementMethod;

public class SimpleDynamoActivity extends Activity {
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);


		final Button LDump = (Button) findViewById(R.id.button1);
		LDump.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				getDataFromCP("LDump");
			}
		});


		final Button GDump = (Button) findViewById(R.id.button2);
		GDump.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				getDataFromCP("GDump");
			}
		});
	}

	public void getDataFromCP(String query){
		final TextView tv = (TextView) findViewById(R.id.textView1);
		tv.setMovementMethod(new ScrollingMovementMethod());
		findViewById(R.id.button3).setOnClickListener(new OnTestClickListener(tv, getContentResolver()));

		Cursor resultCursor = getContentResolver().query(
			SimpleDynamoProvider.CONTENT_URI,
			null,
			query,
			null,
			null
		);

		int i = 0;
		try {
			if (resultCursor.moveToFirst()) {
				while (!resultCursor.isAfterLast()) {
					String key = resultCursor.getString(0);
					String value = resultCursor.getString(1);
					String type = resultCursor.getString(2);
					String port = resultCursor.getString(3);
					String time = resultCursor.getString(4);

					tv.append(i + " KEY: " + key + " \n VALUE: " + value + " " +
							"\n" +
							" " +
							"TYPE: " + type + " \n PORT: "+ port + " \n TIME:" +
							" " + time + "\n\n\n");
					i++;

					resultCursor.moveToNext();
				}
			}
		} catch (Exception e) {
			Log.e(TAG, e.toString());
			return;
		} finally {
			resultCursor.close();
		}
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
