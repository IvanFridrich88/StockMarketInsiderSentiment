from urllib.request import Request, urlopen
import bs4
import requests
import time
import datetime
import pandas



def download_finviz_page_html_code(ticker):
    req = Request(f"https://finviz.com/quote.ashx?t={ticker.lower()}", headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    return webpage


def download_insider_trading_note(link):
    req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    return webpage


class InsiderTrade:
    def __init__(self, ticker="", company="", sector="", date="", name="", relationship="", transaction="", cost="", shares="", value="",
                 shares_total=""):
        self.ticker = ticker
        self.company = company
        self.sector = sector
        self.name = name
        self.date = date
        self.relationship = relationship
        self.transaction = transaction
        self.cost = cost
        self.shares = shares
        self.value = value
        self.shares_total = shares_total

    def to_csv_line(self):
        return self.ticker + ";" + self.transaction + ";" + self.company + ";" + self.sector + ";" + self.date + ";" + self.name + ";" \
               + self.relationship + ";" + self.cost + ";" + self.shares + ";" + self.value \
               + ";" + self.shares_total


def scrap_finviz_page(ticker, company_name, sector):
    html = download_finviz_page_html_code(ticker)
    soup = bs4.BeautifulSoup(html, "html.parser")
    insider_rows = soup.findAll('tr', {'class': "insider-row"})
    insider_trades = []
    for ir in insider_rows:
        cells = ir.findAll('td')
        name = cells[0].find_all('a')[0].text.replace(";", "?")
        relationship = cells[1].text.replace(";", "?")
        transaction = cells[3].text.replace(";", "?")
        cost = cells[4].text.replace(";", "?")
        shares = cells[5].text.replace(",", "").replace(";", "?")
        value = cells[6].text.replace(",", "").replace(";", "?")
        shares_total = cells[7].text.replace(",", "").replace(";", "?")
        date = get_date_of_insider_trade(cells[8].find_all('a')[0].get('href'), delay_ms=1000, max_attempts=1).strftime("%Y-%m-%d")
        insider_trade = InsiderTrade(ticker, company_name, sector, date, name, relationship, transaction, cost, shares, value, shares_total)
        insider_trades.append(insider_trade)
    return insider_trades

def get_date_of_insider_trade(link, delay_ms=10000, max_attempts=10):
    for attempt in range(1, max_attempts + 1):
        try:
            time.sleep(delay_ms / 1000.0)
            resp = requests.get(link, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code != 200:
                print("Error status code: " + str(resp.status_code))
                continue
            soup = bs4.BeautifulSoup(resp.text, "html.parser")
            spans = soup.findAll('span')
            for s in spans:
                if "Date of Earliest Transaction" in s.text:
                    date = datetime.datetime.strptime(s.find_next('span').text, '%m/%d/%Y')
                    return date
        except Exception as ex:
            print(ex)
    raise Exception("Too many attempts!")


def main():

    df = pandas.read_csv('companies.csv')
    df.reset_index()
    df = df.sort_values('Symbol')
    df.reset_index()
    df.to_csv('companies.csv', index=False)

    for index, company in df.iterrows():
        time.sleep(1)
        try:
            insider_trades = scrap_finviz_page(company['Symbol'], company['Name'], company['Sector'])
            for it in insider_trades:
                print(it.to_csv_line())
        except Exception as ex:
            print("Exception during scraping ", company['Symbol'], ex)

if __name__ == "__main__":
    main()
