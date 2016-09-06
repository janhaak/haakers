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

# logging.basicConfig(level=logging.DEBUG)

# parsing arguments
PARSER = argparse.ArgumentParser(description='Client message processor')
PARSER.add_argument('type', help="the type of the code release")
PARSER.add_argument('API_token', help="the individual API token given to your team")
PARSER.add_argument('kind_code', help="the kind code for the messages")
PARSER.add_argument('API_base', help="the base URL for the game API")

ARGS = PARSER.parse_args()

# defining global vars
KIND_CODE = ARGS.kind_code # The codes for different kinds of messages
MESSAGES = {} # A dictionary that contains message parts
API_BASE = ARGS.API_base
# 'https://qq1ttt6sp3.execute-api.us-west-2.amazonaws.com/dev'

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

    # Try to get the parts of the message from the MESSAGES dictionary.
    # If it's not there, create one that has None in both parts
    parts = MESSAGES.get(msg_id, [None, None])

    # store this part of the message in the correct part of the list
    parts[part_number] = data

    # store the parts in MESSAGES
    MESSAGES[msg_id] = parts

    # if both parts are filled, the message is complete
    if None not in parts:
        # app.logger.debug("got a complete message for %s" % msg_id)
        print "have both parts"
        # We can build the final message.
        result = parts[0] + parts[1] + KIND_CODE
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        APP.logger.debug("ID: %s" % msg_id)
        APP.logger.debug("RESULT: %s" % result)
        url = API_BASE + '/' + msg_id
        print url
        print result
        response = requests.post(url, data=result, headers={'x-gameday-token':ARGS.API_token})
        print response

    return 'OK'

if __name__ == "__main__":
    PORT = ARGS.type
    # want the port range to be 8080, 8081...
    PORT = '80'+str(80+int(PORT))
    APP.run(host="0.0.0.0", port=PORT)
