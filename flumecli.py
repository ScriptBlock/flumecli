import requests
import datetime
import argparse
import json
import jwt

config = {}


def checkparams():
    parser = argparse.ArgumentParser(description="Utility for exporting Flume data")

    parser.add_argument("--clientid", help="Flume client API")
    parser.add_argument("--clientsecret", help="Flume client secret")

    parser.add_argument("--username", help="Flume username.  Only required to obtain initial token.")
    parser.add_argument("--password", help="Flume client secret.  Only required to obtain initial token.")

    parser.add_argument("--hecurl", help="Full HEC URL.  e.g. - 'http://172.16.1.2:8088' or 'https://hec.mysplunklab.com:443'")
    parser.add_argument("--hectoken", help="HEC token")
    parser.add_argument("--hecindex", help="Destination HEC index" )
    parser.add_argument("--hecsourcetype", help="Destination HEC sourcetype")


    parser.add_argument("--tokenfile", help="Token details file.  This file will be written to when in --auth mode.  This file will be read from for all other modes.")
    parser.add_argument("--logfile", help="Logfile to write query data to.  If this parameter is not specified, the query data will go to stdout.")

    parser.add_argument("--verbose", "-v", help="Add verbosity", action="store_true")

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument("--auth", help="Obtain authentication token", action="store_true")
    action_group.add_argument("--renew", help="Renew auth token", action="store_true")
    action_group.add_argument("--details", help="Get important metadata about your Flume account", action="store_true")
    action_group.add_argument("--query", help="Query water usage for last minute", action="store_true")

    

    args = parser.parse_args()

    config["clientid"] = args.clientid
    config["clientsecret"] = args.clientsecret
    config["username"] = args.username
    config["password"] = args.password
    config["hecurl"] = args.hecurl
    config["hectoken"] = args.hectoken
    config["hecindex"] = args.hecindex
    config["hecsourcetype"] = args.hecsourcetype
    config["tokenfile"] = args.tokenfile
    config["logfile"] = args.logfile
    config["verbose"] = args.verbose

        
    if args.auth: config["mode"] = "auth"
    if args.details: config["mode"] = "details"
    if args.query: config["mode"] = "query"
    if args.renew: config["mode"] = "renew"
    
    return config

def obtainCredentials(config):
    if config["verbose"]: print("Getting auth token")

    if config["clientid"] and config["clientsecret"] and config["username"] and config["password"]:
        if config["verbose"]: print("all required parameters passed for auth token")
        url = "https://api.flumetech.com/oauth/token"
        payload = '{"grant_type":"password","client_id":"' + config["clientid"] + '","client_secret":"' + config["clientsecret"] + '","username":"' + config["username"] + '","password":"' + config["password"] + '"}'
        headers = {'content-type': 'application/json'}

        resp = requests.request("POST", url, data=payload, headers=headers)
        if config["verbose"]: print("response from server: " + resp.text)
        dataJSON = json.loads(resp.text)

        if dataJSON["http_code"] == 200:
            if config["verbose"]: print("Got 200 response from auth token request")
            config["access_token"] = dataJSON["data"][0]["access_token"]
            config["refresh_token"] = dataJSON["data"][0]["refresh_token"]

            if config["tokenfile"]:
                outline = {}
                outline["access_token"] = config["access_token"]
                outline["refresh_token"] = config["refresh_token"]
                if config["verbose"]: print("Saving access and refresh token to : " + config["tokenfile"])
                if config["verbose"]: print(outline)
                f = open(config["tokenfile"], "w")
                f.write(json.dumps(outline))
                f.close()
        else:
            quit("failed to obtain creds")    


def renewCredentials(config):
    url = "https://api.flumetech.com/oauth/token"
    payload = '{"grant_type":"refresh_token", "refresh_token":"' + config["refresh_token"] + '", "client_id":"' + config["clientid"] + '", "client_secret":"' + config["clientsecret"] + '"}'
    print(payload)
    headers = {'content-type': 'application/json'}    
    resp = requests.request("POST", url, data=payload, headers=headers)
    dataJSON = json.loads(resp.text)
    print(dataJSON)




def loadCredentials(config):
    if not config["tokenfile"]: 
        quit("You have to provide a token file")
    else:
        if config["verbose"]: print("Reading token info from: " + config["tokenfile"])
        f = open(config["tokenfile"], "r")
        tokenDetail = f.readline()
        f.close()
        token = json.loads(tokenDetail)
        config["access_token"] = token["access_token"]
        config["refresh_token"] = token["refresh_token"]



def buildRequestHeader():
    header = {"Authorization": "Bearer " + config["access_token"]}
    return header


def testAuthorizationToken():
    resp = requests.request('GET', "https://api.flumetech.com/users/11382", headers=buildRequestHeader())
    #print(resp.text);
    dataJSON = json.loads(resp.text) 
    return dataJSON["http_code"] == 200

def previousminute():
    return (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')

def currentminute():
    #return (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S');
    return (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S');


def getUserID(config):
    if config["verbose"]: print("Getting user ID from JWT")
    decoded = jwt.decode(config["access_token"], verify=False)
    config["user_id"] = decoded["user_id"]
    if config["verbose"]: 
        print("JWT Details: ")
        print(decoded)

def getDevices(config):
    if config["verbose"]: print("Getting devices")
    resp = requests.request('GET', 'https://api.flumetech.com/users/' + str(config["user_id"]) + '/devices', headers=buildRequestHeader())
    
    dataJSON = json.loads(resp.text)
    
    if config["verbose"]: print("Executed device search")
    
    if dataJSON["http_code"] == 200:
        for bridge in dataJSON["data"]:
            if config["verbose"]: 
                print("JSON Data from device")
                print(dataJSON["data"])
            if bridge["type"] == 2:
                config["device_id"] = bridge["id"]


def getWaterFlowLastMinute():
    payload = '{"queries":[{"request_id":"perminute","bucket":"MIN","since_datetime":"' + previousminute() + '","until_datetime":"' + currentminute() + '","group_multiplier":"1","operation":"SUM","sort_direction":"ASC","units":"GALLONS"}]}'
    #print(payload)
    headers = buildRequestHeader();
    headers["content-type"] = "application/json"
    resp = requests.request("POST", "https://api.flumetech.com/users/" + str(config["user_id"])  + "/devices/" + str(config["device_id"])  + "/query", data=payload, headers=headers)
    data = json.loads(resp.text)
    #print(data)
    if data["http_code"]==200:
        return data["data"][0]["perminute"][0]["value"]
    else:
        return None


def transmitFlow(flowValue):
    if(config["logfile"]):
        if config["verbose"]: print("Sending value to " + config["logfile"] + ":" + str(flowValue))
        f = open(config["logfile"], "a")
        f.write(currentminute() + ": " + str(flowValue) + "\n\r")
        f.close()
    else:
        print(currentminute() + ": " + str(flowValue))

    if config["hecurl"] and config["hectoken"]:
        if config["verbose"]: print("HEC defined, sending to splunk HEC")
        header = {"Authorization": "Splunk " + config["hectoken"]}
        payload = {"event": flowValue}
        if config["hecindex"]: payload["index"] = config["hecindex"]
        if config["hecsourcetype"]: payload["sourcetype"] = config["hecsourcetype"]
        jsonPayload = json.dumps(payload)
        resp = requests.request("POST", config["hecurl"] + "/services/collector/event", data=jsonPayload, headers=header)
        result = json.loads(resp.text)

        if result["text"] == 'Success':
            if config["verbose"]: print("Successfully posted to hEC")
        else:
            if config["verbose"]: print("Failed to send to HEC")

def main():
    global config
    config = checkparams()

    if config["mode"] == "auth":
        obtainCredentials(config)

    if config["mode"] == "renew":
        loadCredentials(config)
        renewCredentials(config)

    if config["mode"] == "details":
        loadCredentials(config)
        getUserID(config)
        getDevices(config)
        print("-------------------------------------------")
        print("Access Token: " + config["access_token"])
        print("Refresh Token: " + config["refresh_token"])
        print("User ID: " + str(config["user_id"]))
        print("Device ID: " + config["device_id"])

    if config["mode"] == "query":
        loadCredentials(config);
        getUserID(config)
        getDevices(config)    
        transmitFlow(getWaterFlowLastMinute())


    
main()
