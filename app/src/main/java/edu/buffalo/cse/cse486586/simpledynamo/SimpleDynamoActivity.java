package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		final TextView tv = (TextView) findViewById(R.id.textView1);
		tv.setMovementMethod(new ScrollingMovementMethod());

		TelephonyManager tel = (TelephonyManager)this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		final Button button1 = (Button) findViewById(R.id.button1);
		final Button button2 = (Button) findViewById(R.id.button2);
		findViewById(R.id.button3).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver(), myPort));

		button1.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				new OnTestClickListener(tv, getContentResolver(), myPort).query("@");
			}
		});

		button2.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				new OnTestClickListener(tv, getContentResolver(), myPort).query("*");
			}
		});
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
