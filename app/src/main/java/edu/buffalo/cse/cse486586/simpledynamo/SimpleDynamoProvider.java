package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = "SimpleDhtProvider";
	private static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String TABLE_NAME = "MESSAGES";
	private String serverPortHash;
	private String myPort;
	private String predPort = null;
	private String succPort = null;
	private static final ArrayList<String> nodeList = new ArrayList<String>();
	private static final HashMap<String, String> hashPortMap = new HashMap<String, String>();
    private static final List<String> portOrders = Arrays.asList("11124","11112","11108",
                                                                "11116","11120");
    private static final List<String> hashOrders = Arrays.asList("177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
            "208f7f72b198dadd244e61801abe1ec3a4857bc9","33d6357cfaaf0f72991b0ecd8c56da066613c089",
            "abf0fd8db03e5ecb199a9b82929e9db79b909643","c25ddd596aa7c81fa12378fa725f706d54325d12");
    private static ArrayList<String> allMessages = new ArrayList<String>();
	private DBHelper dBHelper;
    private static SQLiteDatabase sqLiteDatabase = null;
    private static boolean isRecovering = false;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO delete
        String keyHash = null;
        while (isRecovering){}
        if(selection.equals("*")){
            Message msg = new Message(myPort,selection,"");
            msg.setMessageType("DELETE");
            msg.setSenderPort(myPort);
            for(String port : portOrders){
                try {
                    String resp = new ClientTask().execute(msg.getJSON(), port).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        else if(selection.equals("@")){
            Message msg = new Message(myPort,selection,"");
            msg.setMessageType("DELETE");
            msg.setSenderPort(myPort);
            delete(msg);
        }
        try {
            keyHash = genHash(selection);
            String storePort = getStorePort(keyHash);
            String resp = "crashed";
            int storeIndex = portOrders.indexOf(storePort);
            int count = 0;
            while(resp.equals("crashed") && count < 3){
                Message message = new Message(storePort, selection, "");
                message.setSenderPort(myPort);
                message.setMessageType("DELETE");
                resp = new ClientTask().execute(message.getJSON(), portOrders.get(storeIndex)).get();
                storeIndex++;
                if(storeIndex == portOrders.size()){
                    storeIndex = 0;
                }
                count++;
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
		return 0;
	}

    public synchronized boolean delete(Message msg){
        // TODO actual delete
        String selection = msg.getKey();
        long rowId;

        if(msg.getKey() == null || msg.getKey().equals(""))
            return false;

        if(selection.equals("@") || selection.equals("*")){
            rowId = sqLiteDatabase.delete(TABLE_NAME, "1", null);
            if(rowId > 0){
                Log.e("DELETED",String.valueOf(rowId) + " rows");
            }
        }
        else{

            String[] actualSelectionArgs = {selection};
            rowId = sqLiteDatabase.delete(TABLE_NAME,"key=?",actualSelectionArgs);
            Log.e("DELETE ID" , selection + " " + rowId);
        }


        // return response
        return true;
    }

	@Override
	public String getType(Uri uri) {
		// TODO getType
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO insert
        String keyHash = null;
        while (isRecovering){}
        Log.e("INSRT", "called");
        if (values.containsKey(KEY_FIELD) && values.containsKey(VALUE_FIELD)) {
            Log.e("INSERT", values.getAsString(KEY_FIELD) +  " " + values.getAsString(VALUE_FIELD));

            try {
                keyHash = genHash(values.getAsString(KEY_FIELD));
                String resp = "crashed";
                String storePort = getStorePort(keyHash);
                int storeIndex = portOrders.indexOf(storePort);
                int count = 0;
                while(resp.equals("crashed") && count < 3){
                    Message message = new Message(storePort, values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD));
                    message.setSenderPort(myPort);
                    message.setMessageType("STORE");
                    resp = new ClientTask().execute(message.getJSON(), portOrders.get(storeIndex)).get();
                    storeIndex++;
                    if(storeIndex == portOrders.size()){
                        storeIndex = 0;
                    }
                    count++;
                }


            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return null;
	}

    public synchronized boolean insert(Message msg){
        // TODO actual insert
        String keyHash = "";
        if(msg.getKey() == null || msg.getKey().equals("") || msg.getValue() == null || msg.getValue().equals(""))
            return false;
        Log.e("ACTUAL INSERT", msg.getKey() + " " + msg.getValue());

//        try{
//            keyHash = genHash(msg.getKey());
//        }catch (NoSuchAlgorithmException e){
//            e.printStackTrace();
//        }
        // insert in local if no other avd present or value belongs to the region
        ContentValues insValues = new ContentValues();
        insValues.put(KEY_FIELD, msg.getKey());
        insValues.put(VALUE_FIELD, msg.getValue());
        long rowId;
        try{
            rowId = sqLiteDatabase.insertOrThrow(TABLE_NAME, null, insValues);
        }
        catch (SQLiteException exception){
            String[] args = {insValues.getAsString(KEY_FIELD)};
            rowId = sqLiteDatabase.update(TABLE_NAME,insValues,"key = ?",args);
        }

        if (rowId > 0) {
            return true;
        }
        return false;
    }

	@Override
	public boolean onCreate() {
		// TODO onCreate
		dBHelper = new DBHelper(getContext());
        sqLiteDatabase = dBHelper.getWritableDatabase();
        allMessages = new ArrayList<String>();

		TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		String resp = null;


		try{
            serverPortHash = genHash(portStr);
            List<String> neighbours = getNeighbours(myPort);
            predPort = neighbours.get(0);
            succPort = neighbours.get(1);
            Log.e(TAG, predPort + " " + myPort + " " + succPort);
            Log.e(TAG, serverPortHash);

            ServerSocket socket = new ServerSocket(SERVER_PORT);
			socket.setReuseAddress(true);
            isRecovering = true;
//            socket.setSoTimeout(5000);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);
            new RecoveryTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
		}catch (IOException e){
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

		// If you need to perform any one-time initialization new task, please do it here.
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO query
        while (isRecovering){}
        String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        HashMap<String, String> msgMap = new HashMap<String, String>();
        String resp = null;
        String delim = ",,,";
        Log.e("QUERY", selection);

        Cursor cursor = null;

        //if query is not * then perform on local
        if(selection.equals("*")){
            Message message = new Message(myPort,selection,"");
            message.setMessageType("QUERY");
            message.setSenderPort(myPort);
            for(String port : portOrders){
                if(port.equals(myPort))
                    continue;
                try {
                    if(resp == null){
                        resp = new ClientTask().execute(message.getJSON(), port).get();
                    }
                    else{
                        resp = resp + delim + new ClientTask().execute(message.getJSON(), port).get();
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            selection = "@";
        }
        if(selection.equals("@")){
            Message message = new Message(myPort,selection,"");
            message.setMessageType("QUERY");
            message.setSenderPort(myPort);
            cursor = query(message);
//            Log.e("SIZE", "" + cursor.getCount());
        }
        else{
            String keyHash = "";
            try{
                keyHash = genHash(selection);
                String storePort = getStorePort(keyHash);
                int storeIndex = (portOrders.indexOf(storePort) + 2) % 5;
                if (storeIndex == portOrders.indexOf(myPort)){
                    Message message = new Message(myPort,selection,"");
                    message.setMessageType("QUERY");
                    message.setSenderPort(myPort);
                    cursor = query(message);
                }
                else{
                    resp = "crashed";
                    int count = 0;
                    while(resp.equals("crashed") && count < 3){
                        Message message = new Message(storePort,selection,"");
                        message.setMessageType("QUERY");
                        message.setSenderPort(myPort);
                        Log.e("QUERYING", portOrders.get(storeIndex));
                        resp = new ClientTask().execute(message.getJSON(), portOrders.get(storeIndex)).get();
                        storeIndex--;
                        if(storeIndex == -1){
                            storeIndex = portOrders.size()-1;
                        }
                        count++;
                    }
                }
            }
            catch (NoSuchAlgorithmException e){
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        if( cursor != null && cursor.getColumnCount() == 2){
            if(cursor.moveToFirst()){
                do{
                    int keyColValue = cursor.getColumnIndex("key");
                    int valueColValue = cursor.getColumnIndex("value");
                    String key = null;
                    String value = null;
                    try{
                        key = cursor.getString(keyColValue);
                        value = cursor.getString(valueColValue);

                    }
                    catch (CursorIndexOutOfBoundsException exception){
                        Log.e(TAG,selection);
                        Log.e(TAG,selection.getClass().toString());
                    }
                    Object[] columnValues = {key, value};
                    matrixCursor.addRow(columnValues);
                }
                while (cursor.moveToNext());

            }

        }
        //get response from successor if any
        if(resp != null && !resp.equals("")){
            String[] results = resp.split(delim);
            for(String result : results){
//                Log.e("ALL", result);
                HashMap<String, String> resultMap = getMap(result);
                for(String key : resultMap.keySet()){
                    if(resultMap.get(key) != null){
                        Object[] columnValues = {key, resultMap.get(key)};
                        matrixCursor.addRow(columnValues);
                    }
                }
            }

        }
        //if cursor got some rows then return
        return matrixCursor;
	}

    public synchronized Cursor query(Message message){
        // TODO actual query
        String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        Cursor cursor = null;

        String query = message.getKey();
//        Log.e("QUERY KEY", message.getKey());
        Log.e("HELPER QUERY", message.getJSON());
        if(query.equals("*") || query.equals("@")){
            cursor = sqLiteDatabase.rawQuery("SELECT * FROM " + TABLE_NAME, null);
            Log.e("size", cursor.getCount() + "");
        }
        else{
            String[] actualSelectionArgs = {query};
            cursor = sqLiteDatabase.query(TABLE_NAME, columnNames, "key= ?", actualSelectionArgs,
                    null, null, null);
        }
        if( cursor != null && cursor.getColumnCount() == 2){
            if(cursor.moveToFirst()){
                do{
                    int keyColValue = cursor.getColumnIndex(KEY_FIELD);
                    int valueColValue = cursor.getColumnIndex(VALUE_FIELD);
                    String key = null;
                    String value = null;
                    try{
                        key = cursor.getString(keyColValue);
                        value = cursor.getString(valueColValue);

                    }
                    catch (CursorIndexOutOfBoundsException exception){
                        Log.e(TAG,query);
                        Log.e(TAG,query.getClass().toString());
                    }
                    Object[] columnValues = {key, value};
                    matrixCursor.addRow(columnValues);
                }
                while (cursor.moveToNext());

            }

        }

        //if cursor got some rows then return
        return matrixCursor;

    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO update
		return 0;
	}

    private synchronized String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class RecoveryTask extends AsyncTask<Void, Void, Void> {
        @Override
        // TODO recoveryTask
        protected Void doInBackground(Void... voids) {
            isRecovering = true;
            Log.e("RECOVERY","entered");
            Socket socket = null;
            String receivedMessage = null;
            Message message = new Message(myPort,"","");
            message.setSenderPort(myPort);
            message.setMessageType("RECOVER");
            OutputStream sendStream;
            DataOutputStream sendData;
            DataInputStream recvData;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(predPort));
                socket.setSoTimeout(5000);
                sendStream = socket.getOutputStream();
                sendData = new DataOutputStream(sendStream);
                sendData.writeUTF(message.getJSON());
                sendData.flush();
                recvData = new DataInputStream(socket.getInputStream());
                receivedMessage = recvData.readUTF();
                processMessages(receivedMessage);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                Log.e("RECOVERY", "done pred " + isRecovering);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(succPort));
                socket.setSoTimeout(5000);
                sendStream = socket.getOutputStream();
                sendData = new DataOutputStream(sendStream);
                sendData.writeUTF(message.getJSON());
                sendData.flush();
                recvData = new DataInputStream(socket.getInputStream());
                receivedMessage = recvData.readUTF();
                processMessages(receivedMessage);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Log.e("RECOVERY", "done succ " + isRecovering);
            isRecovering = false;
            Log.e("RECOVERY","exiting " + isRecovering);
            return null;
        }

    }

    private class ClientTask extends AsyncTask<String, Void, String> {
        @Override
        // TODO client
        protected String doInBackground(String... msgs) {
            String message = msgs[0];
            String remotePort = msgs[1];
            Socket socket = null;
            String receivedMessage = "";
            Log.e("CLIENT", message + " " + remotePort);
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                socket.setSoTimeout(5000);
                OutputStream sendStream = socket.getOutputStream();
                DataOutputStream sendData = new DataOutputStream(sendStream);
                sendData.writeUTF(message);
                sendData.flush();
                DataInputStream recvData = new DataInputStream(socket.getInputStream());
                receivedMessage = recvData.readUTF();
            } catch (IOException e) {
                //e.printStackTrace();
                return "crashed";
            }
            return receivedMessage;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        // TODO server
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket;

            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
            uriBuilder.scheme("content");
            Uri mUri = uriBuilder.build();
            Log.e("SOCKET","STARTED");

            while (isRecovering){};


            while (true) {

                try {
                    socket = serverSocket.accept();
                    DataInputStream receivedStream = new DataInputStream(socket.getInputStream());
                    String receivedMessage = receivedStream.readUTF();
                    DataOutputStream sendData = new DataOutputStream(socket.getOutputStream());

                    Message msg = new Message(receivedMessage);

                    String storePort = msg.getStorePort();
                    String senderPort = msg.getSenderPort();
                    String type = msg.getMessageType();
                    int distance = getDistance(myPort, storePort);
                    int index = portOrders.indexOf(myPort);
                    Log.e("SOCKET", msg.getJSON());

                    while(isRecovering){
                        Thread.sleep(50);
                    }

                    if(type.equals("RECOVER")){
                        isRecovering = true;
                        sendData.writeUTF(getMissedMessages());
                        isRecovering = false;
                    }

                    else if(type.contains("STORE")){
                        if(distance < 3){
                            addMessage(msg);
                            insert(msg);
                            int tempIndex = (index + 1) % 5;
                            if(distance < 2){
                                while(true){
                                    if(getDistance(portOrders.get(tempIndex), storePort) < 3){
                                        try{
                                            Log.e("SENDING", distance + " " + portOrders.get(tempIndex));
                                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                    Integer.parseInt(portOrders.get(tempIndex)));
                                            socket.setSoTimeout(5000);
                                            DataOutputStream sendSuccData = new DataOutputStream(socket.getOutputStream());
                                            sendSuccData.writeUTF(receivedMessage);
                                            sendData.flush();
                                            DataInputStream recvSuccData = new DataInputStream(socket.getInputStream());
                                            String recvMessage = recvSuccData.readUTF();
                                            if(Boolean.parseBoolean(recvMessage)){
                                                break;
                                            }
                                        }
                                        catch (IOException exception){
                                            Log.e("CRASHED", portOrders.get(tempIndex));
                                        }
                                        tempIndex = (tempIndex + 1) % 5;

                                    }
                                    else{
                                        break;
                                    }

                                }

                            }
                        }
                        sendData.writeUTF("true");
                    }
                    else if(type.contains("QUERY")){
                        HashMap<String,String> resultMap = new HashMap<String, String>();
                        if(!msg.getKey().equals("*") && !msg.getKey().equals("@")){
                            if(distance < 3 && distance >= 0){
                                sendData.writeUTF(convertCursorToString(query(msg),resultMap));
                            }
                        }
                        else{
                            sendData.writeUTF(convertCursorToString(query(msg),resultMap));
                        }
                        sendData.writeUTF("{}");

                    }
                    else if(type.contains("DELETE")){
                        if(!msg.getKey().equals("*") && !msg.getKey().equals("@")){
                            if(distance < 3){
                                addMessage(msg);
                                delete(msg);
                                int tempIndex = (index + 1) % 5;
                                if(distance < 2){
                                    while(true){
                                        if(getDistance(portOrders.get(tempIndex), storePort) < 3){
                                            try{
                                                Log.e("SENDING", distance + " " + portOrders.get(tempIndex));
                                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                        Integer.parseInt(portOrders.get(tempIndex)));
                                                socket.setSoTimeout(5000);
                                                DataOutputStream sendSuccData = new DataOutputStream(socket.getOutputStream());
                                                sendSuccData.writeUTF(receivedMessage);
                                                sendData.flush();
                                                DataInputStream recvSuccData = new DataInputStream(socket.getInputStream());
                                                String recvMessage = recvSuccData.readUTF();
                                                if(Boolean.parseBoolean(recvMessage)){
                                                    break;
                                                }
                                            }
                                            catch (IOException exception){
                                                Log.e("CRASHED", portOrders.get(tempIndex));
                                            }
                                            tempIndex = (tempIndex + 1) % 5;

                                        }
                                        else{
                                            break;
                                        }

                                    }

                                }
                            }
                        }
                        else{
                            addMessage(msg);
                            delete(msg);
                        }

                        sendData.writeUTF("true");
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    public synchronized String getStorePort(String hash){
        for(int i=0;i<hashOrders.size();i++){
            if(hash.compareTo(hashOrders.get(i)) <= 0)
                return portOrders.get(i);
        }
        return portOrders.get(0);
    }

    public synchronized List<String> getNeighbours(String portId){
        int portIndex = portOrders.indexOf(portId);
        ArrayList<String> neighbours = new ArrayList<String>(2);


        Integer predIndex = portIndex == 0 ? portOrders.size() - 1 : portIndex - 1;
        Integer succIndex = portIndex == portOrders.size() - 1 ? 0 : portIndex + 1;
        neighbours.add(portOrders.get(predIndex));
        neighbours.add(portOrders.get(succIndex));
        return neighbours;
    }

    public synchronized int getDistance(String myPort, String storePort){
        int myIndex = portOrders.indexOf(myPort);
        int storePortIndex = portOrders.indexOf(storePort);
        int distance = myIndex - storePortIndex;
        if(distance < 0)
            distance += portOrders.size();
        return distance;
    }

    public synchronized String convertCursorToString(Cursor resultCursor, HashMap<String,String> resultMap){
        //convert cursor object to json string to be sent on network
        if (resultCursor == null) {
            Log.e(TAG, "Result null");

        }
        if(resultCursor != null && resultCursor.getCount() > 0){
            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
            if (keyIndex == -1 || valueIndex == -1) {
                Log.e("convertCursorToString", "Wrong columns");
            }

            else if(resultCursor.moveToFirst()){
                do{
                    keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                    valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                    String returnKey = resultCursor.getString(keyIndex);
                    String returnValue = resultCursor.getString(valueIndex);
                    resultMap.put(returnKey, returnValue);
                }
                while (resultCursor.moveToNext());
            }
        }
        return new JSONObject(resultMap).toString();
    }

    public synchronized HashMap<String, String> getMap(String jsonString){
        //convert hashmap to json string to be sent on network
        HashMap<String, String> map = new HashMap<String, String>();
        try{
            JSONObject jObject = new JSONObject(jsonString);
            Iterator<String> keys = jObject.keys();

            while( keys.hasNext() ){
                String key = (String)keys.next();
                String value = jObject.getString(key);
                map.put(key, value);

            }
        }
        catch (JSONException e){
            e.printStackTrace();
        }

        return map;

    }

    public synchronized Void processMessages(String recievedMessages){
        String[] messages = recievedMessages.split(",,,,");
        if(recievedMessages == null || recievedMessages.equals(""))
            return null;
        for(String message : messages){
//            Log.e("RECOVERY", message);
            Message msg = new Message(message);
            String storePort = null;
            try {
                storePort = getStorePort(genHash(msg.getKey()));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if(storePort != null && getDistance(myPort,storePort) < 3){
                if(msg.getMessageType().contains("STORE")){
                    allMessages.add(msg.getMinJson());
                    insert(msg);
                }
                if(msg.getMessageType().contains("DELETE")){
                    allMessages.add(msg.getMinJson());
                    delete(msg);
                }
            }
        }
        return null;
    }

    public synchronized String getMissedMessages(){
        return TextUtils.join(",,,,",allMessages);
    }

    public synchronized Void addMessage(Message message){
        allMessages.add(message.getMinJson());
        return null;
    }


}
