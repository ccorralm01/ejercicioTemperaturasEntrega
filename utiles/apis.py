import requests
def consulta_api_paises(endpoint):

    url = f"https://restcountries.com/v3.1/{endpoint}"
    res = requests.get(url)

    return res.content

def consulta_api_temperaturas(type, endpoint):
    if type == "JSON":
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={endpoint[1]}&lon={endpoint[2]}&appid=a60fef3fc371c50c0c57c17361925fad"
    else:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={endpoint[1]}&lon={endpoint[2]}&mode=xml&appid=a60fef3fc371c50c0c57c17361925fad"

    res = requests.get(url)

    return res.content


