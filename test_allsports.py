import requests
import json
from datetime import date, timedelta

API_KEY = "3c9d66e88c5a6fbe2a9632b838710ce51c8838ccb734ad188bb6639ff2b02836"
BASE_URL = "https://apiv2.allsportsapi.com/tennis/"

today = date.today()
tomorrow = today + timedelta(days=1)

fixtures_url = f"{BASE_URL}?met=Fixtures&APIkey={API_KEY}&from={today}&to={tomorrow}"
f_resp = requests.get(fixtures_url).json()

odds_url = f"{BASE_URL}?met=Odds&APIkey={API_KEY}&from={today}&to={tomorrow}"
o_resp = requests.get(odds_url).json()

print(f"Fixtures count: {len(f_resp.get('result', []))}")

odds_result = o_resp.get('result', {})
print(f"Odds keys count: {len(odds_result.keys()) if isinstance(odds_result, dict) else 0}")

# check one match 
if f_resp.get("result"):
    for m in f_resp["result"]:
        if " / " in m["event_first_player"] or " / " in m["event_second_player"]:
            continue # skip doubles
        if m["event_status"] in ["Finished", "Cancelled"]:
            continue
        odd_data = odds_result.get(str(m["event_key"]))
        if odd_data:
            print("Upcoming match WITH ODDS:")
            print(m["event_key"], m["event_first_player"], "vs", m["event_second_player"])
            print("Odds type:", type(odd_data))
            if isinstance(odd_data, list):
                print(json.dumps(odd_data, indent=2)[:500])
            else:
                 print(json.dumps(odd_data, indent=2)[:500])
            break
