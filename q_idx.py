# import requests

# url = "https://morning-star.p.rapidapi.com/stock/v2/get-financials"

# querystring = {"performanceId":"0P0000OQN8","interval":"annual","reportType":"A"}

# headers = {
# 	"X-RapidAPI-Key": "6ab3672a06msh2ed5f438f0b0778p16617fjsnf2d8ec4cde43",
# 	"X-RapidAPI-Host": "morning-star.p.rapidapi.com"
# }

# response = requests.get(url, headers=headers, params=querystring)

# print(response.json())

import requests

url = "https://morning-star.p.rapidapi.com/stock/v2/get-financial-details"

# querystring = {"performanceId":"0P0000EP1E","dataType":"A","reportType":"A","type":"incomeStatement"}
querystring = {"performanceId":"0P0000EP1E","dataType":"Q","reportType":"A","type":"incomeStatement"}

headers = {
	"X-RapidAPI-Key": "6ab3672a06msh2ed5f438f0b0778p16617fjsnf2d8ec4cde43",
	"X-RapidAPI-Host": "morning-star.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())