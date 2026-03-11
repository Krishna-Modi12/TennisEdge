import requests

url = "https://tennisapi1.p.rapidapi.com/api/tennis/rankings/atp"
headers = {
    "X-RapidAPI-Key": "c413ab2d11msha135da281753633p19881ejsn83d7601a4708",
    "X-RapidAPI-Host": "tennisapi1.p.rapidapi.com",
}

r = requests.get(url, headers=headers, timeout=30)
print(r.status_code, r.text[:500])
