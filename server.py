#!/usr/bin/env python
"""
Client which receives the requests

Args:
    type (1-10)
    API Token
    Kind Code (abcd, efgh, etc.)
    API Base (https://...)

"""
from flask import Flask, request
import logging
import argparse
import requests
import boto3
from boto3.dynamodb.conditions import Key

#logging.basicConfig(level=logging.INFO)

# parsing arguments
PARSER = argparse.ArgumentParser(description='Client message processor')
PARSER.add_argument('type', help="the type of the code release")
PARSER.add_argument('API_token', help="the individual API token given to your team")
PARSER.add_argument('kind_code', help="the kind code for the messages")
PARSER.add_argument('API_base', help="the base URL for the game API")

ARGS = PARSER.parse_args()

# defining global vars
KIND_CODE = ARGS.kind_code # The codes for different kinds of messages
KIND_CODE = "someone is reading this, right?"
MESSAGES = {} # A dictionary that contains message parts
API_BASE = ARGS.API_base
DYNAMODB = boto3.resource('dynamodb') # creating dynamo resource
STATE_TABLE = DYNAMODB.Table('gameday-messages-state') # creating state table object


APP = Flask(__name__)

# creating flask route for type argument
@APP.route('/'+ARGS.type, methods=['GET', 'POST'])
def main_handler():
    """
    main routing for requests
    """
    if request.method == 'POST':
        return process_message(request.get_json())

    else:
        return get_message_stats()

def store_message(input_id, part_num, data):
    """
    stores the message locally on a file on disk for persistence
    """
    # putting record into dynamo with the part number received
    STATE_TABLE.update_item(
        Key={
            'Id': input_id
        },
        UpdateExpression="set #key=:val",
        ExpressionAttributeValues={
            ":val":data
        },
        ExpressionAttributeNames={
            "#key":str(part_num)
        }
    )

def check_messages(input_id):
    """
    checking to see in dynamo if we have the part already
    """
    # do a scan of dynamo to see if item exists
    response = STATE_TABLE.scan(FilterExpression=Key('Id').eq(input_id))

    # checking if the object was returned
    if len(response['Items']) == 0:
        # message doesn't exist, so move on
        # this should never happen since we
        # put the initial entry in dynamo before
        # checking...but just in case
        return
    else:
        item = response['Items'][0]
        # check if both parts exist
        if "0" in item and "1" in item:
            # we have all the parts
            print "have all parts"
            # proceed to putting items together and returning
            build_final(item, input_id)
            return
        else:
            # we have some parts but not all
            print "have some parts"
            return

def build_final(parts, msg_id):
    """
    building the response to return to the server
    """
    # We can build the final message.
    result = parts['0'] + parts['1'] + KIND_CODE
    # sending the response to the score calculator
    # format:
    #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
    #   headers -> x-gameday-token = API_token
    #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
    APP.logger.debug("ID: %s" % msg_id)
    APP.logger.debug("RESULT: %s" % result)
    url = API_BASE + '/' + msg_id
    requests.post(url, data=result, headers={'x-gameday-token':ARGS.API_token})

def get_message_stats():
    """
    provides a status that players can check
    """
    msg_count = len(MESSAGES.keys())
    return "There are %d messages in the MESSAGES dictionary" % msg_count

def process_message(msg):
    """
    processes the messages by combining and appending the kind code
    """
    msg_id = msg['Id'] # The unique ID for this message
    part_number = msg['PartNumber'] # Which part of the message it is
    data = msg['Data'] # The data of the message

    # put the part received into dynamo
    store_message(msg_id, part_number, data)

    # Try to get the parts of the message from the Dynamo.
    check_messages(msg_id)
    return 'OK'

if __name__ == "__main__":
    PORT = ARGS.type
    # want the port range to be 8080, 8081...
    PORT = '80'+str(80+int(PORT))
    APP.run(host="0.0.0.0", port=PORT)
