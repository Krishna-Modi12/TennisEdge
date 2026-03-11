import requests

url = "https://tennis-api-atp-wta-itf.p.rapidapi.com/tennis/player-stats"
headers = {
    "X-RapidAPI-Key": "c413ab2d11msha135da281753633p19881ejsn83d7601a4708",
    "X-RapidAPI-Host": "tennis-api-atp-wta-itf.p.rapidapi.com",
}

r = requests.get(url, headers=headers, timeout=30)
print(r.status_code, r.text[:500])
