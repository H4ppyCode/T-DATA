import requests
import bs4

def bs_find_all(url, element, attribute=None, value=None, user_agent=None, proxies=None, features='html.parser', **kwargs):
    if user_agent:
        page = requests.get(url, proxies=proxies, headers={'User-Agent':'%s' % user_agent}, **kwargs).text
    else:
        page = requests.get(url, proxies=proxies, **kwargs).text
    soup = bs4.BeautifulSoup(page, features=features)
    if value:
        return soup.find_all(element, {attribute:value})
    else:
        return soup.find_all(element)
    