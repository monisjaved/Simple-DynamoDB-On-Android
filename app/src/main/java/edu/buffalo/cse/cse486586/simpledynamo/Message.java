package edu.buffalo.cse.cse486586.simpledynamo;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by moonisjaved on 3/10/17.
 */


public class Message {
    private String storePort;
    private String key;
    private String value;
    private String messageType;
    private String senderPort;


    public Message(String id, String key, String msg){
        this.storePort = id;
        this.value = msg;
        this.key = key;
        this.messageType = "NEW";
    }

    public Message(String jsonString){
        JSONObject json = null;
        try {
            json = new JSONObject(jsonString);
            if(json.has("storePort"))
                this.storePort = json.get("storePort").toString();
            this.value = json.get("value").toString();
            this.messageType = json.get("messageType").toString();
            this.key = json.get("key").toString();
            if(json.has("senderPort"))
                this.senderPort = json.get("senderPort").toString();
        } catch (JSONException e) {
            String[] res = jsonString.split(",,");
            this.key = res[0];
            this.value = res[1];
            this.messageType = res[2];
        }

    }

    public void setValue(String value) { this.value = value; }

    public String getSenderPort() { return this.senderPort; }

    public void setSenderPort(String senderPort) { this.senderPort = senderPort; }

    public String getKey(){
        return this.key;
    }

    public String getStorePort(){
        return this.storePort;
    }

    public String getValue(){
        return this.value;
    }

    public void setMessageType(String val){
        this.messageType = val;
    }

    public String getMessageType() {
        return this.messageType;
    }

    public void setStorePort(String storePort) { this.storePort = storePort; }

    public String getString(){
        String result = "";
        result += this.value + ":::" + this.storePort + ":::" +
                this.key + ":::" + this.messageType;
        return result;
    }

    public String getJSON(){
        Map<String, String> jsonMap = new HashMap<String, String>();
        jsonMap.put("storePort",String.valueOf(this.storePort));
        jsonMap.put("key", this.key);
        jsonMap.put("value",String.valueOf(this.value));
        jsonMap.put("messageType",String.valueOf(this.messageType));
        jsonMap.put("senderPort", this.senderPort);
        JSONObject json = new JSONObject(jsonMap);
        return json.toString();
    }

    public String getMinJson(){
        Map<String, String> jsonMap = new HashMap<String, String>();
//        jsonMap.put("key", this.key);
//        jsonMap.put("value",String.valueOf(this.value));
//        jsonMap.put("messageType", String.valueOf(this.messageType));
//        JSONObject json = new JSONObject(jsonMap);
//        return json.toString();
        return this.key + ",," + this.value + ",," + this.messageType;
    }
}
