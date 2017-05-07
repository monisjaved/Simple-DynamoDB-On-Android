package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by moonisjaved on 4/4/17.
 */

public class DBHelper extends SQLiteOpenHelper {


    private static final String TAG = "DBHelper";
    private static final String DATABASE_NAME = "simpledht.db";
    private static final int DATABASE_VERSION = 1;
    private static final String TABLE_NAME = "MESSAGES";

    DBHelper(Context context){
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        try{
            sqLiteDatabase.execSQL("CREATE TABLE " + TABLE_NAME + "("
                    + "key TEXT PRIMARY KEY,"
                    + "value TEXT"
                    + ")");
        }
        catch (SQLiteException exception){

            Log.e(TAG, exception.getMessage());
            exception.printStackTrace();
//                Log.e(TAG, exception.getStackTrace().toString());
        }

        Log.d(TAG, "Created DB");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        Log.w(TAG, "Upgrading database from version " + i + " to "
                + i1 + ", which will destroy all old data");
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(sqLiteDatabase);
    }
}
