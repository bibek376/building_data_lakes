import requests
import json

def json_data():
    country_name = "Nepal"
    token = "cb30e0dc15aa462246502f6d67a73bd8"
    # Define the API endpoint
    api_url = f"https://api.openweathermap.org/data/2.5/weather?q={country_name}&appid={token}"

    # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Return the JSON data directly
        return response.json()
    else:
        # Print an error message if the request was not successful
        print("Error:", response.status_code)
        # Return None if there's an error
        return None

data = json_data()

# Check if data is not None (i.e., the request was successful)
if data:
    # Write JSON data to a file
    with open("/home/bibek/data.json", "w") as json_file:
        json.dump(data, json_file, indent=4)
    print("JSON data saved to 'data.json'")
