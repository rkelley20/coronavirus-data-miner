import requests
from bs4 import BeautifulSoup
import pdf_parser


def get_latest_report(towns):
    """Parse the HTML to get latest REPORT URL"""
    for row in towns:
        data = row.find_all('a', attrs = {'target':'_blank'})
        for item in data:
            return f"https://www.who.int{item['href']}"
    raise Exception


def main():
    WHO_URL = "https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports"

    # requests situation report website
    covid_reports = requests.get(WHO_URL)
    covid_soup = BeautifulSoup(covid_reports.text, "html.parser")

    # find the specific divs
    towns = covid_soup.find_all("div", class_="sf-content-block content-block")
    
    # find the report name
    report = get_latest_report(towns)

    # get the name to be used as the pdf file
    name = report.split('situation-reports/')[1].split(".pdf")[0]
    covid_request = requests.get(report)

    # download pdf
    with open(f"{name}.pdf", "wb") as f:
        f.write(covid_request.content)

    # scrape the pdf
    pdf_parser.scrape_pdf(name)

if __name__ == "__main__":
    main()