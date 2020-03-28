import os
import requests
import pdf_parser
from pdf_exceptions import PDFScrapeException, PDFNotFoundException
from bs4 import BeautifulSoup


def get_latest_report(towns: BeautifulSoup) -> str:
    """Find the latest report from beautful soup

    :param path: towns
    :param type: BeautifulSoup

    :returns: URL for PDF, can raise PDFNotFoundException
    :return type:
    """
    for row in towns:
        # break
        data = row.find_all('a', attrs = {'target':'_blank'})
        for item in data:
            return f"https://www.who.int{item['href']}"
    raise PDFNotFoundException("Unable to find WHO situation report.")


def pull_who_csv(path: str="") -> str:
    """Create WHO CSV file for usage.

    :param path: where to download PDF & CSV.
    :param type: str

    :returns: csv path, pdf path. Can Raise either PDFScrapeException or PDFNotFoundExceptions
    """

    WHO_URL = "https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports"

    # requests situation report website
    covid_reports = requests.get(WHO_URL)
    covid_soup = BeautifulSoup(covid_reports.text, "html.parser")

    # find the specific divs
    towns = covid_soup.find_all("div", class_="sf-content-block content-block")
    
    # find the report name
    report = None
    report = get_latest_report(towns)

    # get the name to be used as the pdf file
    name = report.split('situation-reports/')[1].split(".pdf")[0]
    covid_request = requests.get(report)

    # download pdf
    who_path_pdf = f"{path}{name}.pdf"
    with open(who_path_pdf, "wb") as f:
        f.write(covid_request.content)
    
    who_path_csv = who_path_pdf.replace(".pdf", ".csv")

    # scrape the pdf
    who_pdf_problem = pdf_parser.scrape_pdf(who_path_pdf, who_path_csv)

    if who_pdf_problem == True:
        raise PDFScrapeException("WHO Changed PDF Format")
    
    return who_path_csv, who_path_pdf

print(pull_who_csv(path="test/"))
