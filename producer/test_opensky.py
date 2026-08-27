import requests

url = "https://opensky-network.org/api/states/all"

response = requests.get(url, timeout=30)

print("Status:", response.status_code)

if response.ok:
    data = response.json()

    print("Time:", data.get("time"))
    print("Number of aircraft:", len(data.get("states", [])))

    if data.get("states"):
        print("First aircraft:")
        print(data["states"][0])
else:
    print(response.text)