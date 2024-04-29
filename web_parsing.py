import requests
from bs4 import BeautifulSoup

r = requests.get("https://www.enzyme-database.org/contents.php")


soup = BeautifulSoup(r.content, "html.parser")

count = 0
for line in soup.stripped_strings:
    print(line)
    count += 1
    if count == 300:
        break
