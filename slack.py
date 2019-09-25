import slack

sc = None

def get_sc(config):
    global sc
    if sc is None:
        sc = slack.SlackClient(config["SLACK"]["token"])
    return sc

def post_message(config, msg):
    get_sc(config).api_call(
        "chat.postMessage",
        channel=config["SLACK"]["channel"],
        text=msg
        #as_user="scooter_scrapper"
    )



