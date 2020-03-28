import PyPDF2
import csv
from pdf_exceptions import PDFEOFException, PDFNewFileFormatException

def remove_bad_headers(data:str) -> str:
    """Remove bad headers from CSV data

    :param data: data to remove headers from
    :param type: str

    :returns: string without bad headers.
    :return type: str
    """
    headers = [
        'Territories\n**\n',
        'European Region',
        'European \nRegion ',
        'South\n-\nEast Asia Region',
        'Eastern Mediterranean Region',
        'Region of the Americas',
        'African Region',
        'European Region '
    ]

    # used to check if the header is in the string
    for item in headers:
        try:
            fix = data.split(item)      # split the item because we don't need it
            fix[1] = fix[1].lstrip()    # strip the headers to fix the problem
            data = ''.join(fix)         # rejoin the string 
        except IndexError:
            pass 

    fix = data.replace(
            "\nLocal\n \ntransmission\n","\nLocal transmission\n"
    ).replace("\nImported cases\n \nonly\n","\nImported cases only\n")
    return fix

def chunker(data: list, chunksize=7) -> list:
    """Split up into chunks for putting into CSV

    :param data: data to split up
    :param type: list

    :returns: chunk for each page
    :return type: list
    """
    chunks = []
    for i in range(0, len(data), chunksize):
        chunks.append(data[i:i+chunksize])

    return chunks

def find_last_pages(i: int, num_pages: int, pdf_reader) -> list:
    """Parses through each page.

    :param i: starting page number
    :param type: int

    :returns: the list of all the data parsed
    :return type: list
    """
    all_chunks = []
    curr = i
    while curr < num_pages:
        page_data = pdf_reader.getPage(curr).extractText()

        if "Grand total" in page_data: # if last page
            good_data = remove_bad_headers(page_data)
            last_chunk = chunker(good_data.split("\n \n"))

            # get rid of everything after last country
            for idx, item in enumerate(last_chunk):
                for sub_idx, sub_item in enumerate(item):
                    if sub_item == "Subtotal for all \nregions":
                        del last_chunk[idx:]

            all_chunks.append(last_chunk)
            return all_chunks
        else:
            if curr == i: # used for the first page
                good_data = remove_bad_headers(page_data)
                # print(good_data)
                # gets the first region of data and removes western pacific region heading
                all_chunks.append(
                    chunker(good_data.split("Western Pacific Region")[1].split("\n \n")[1:]
                ))
                
            else:
                # normal page of the table usually in middle
                good_data = remove_bad_headers(page_data)
                all_chunks.append(chunker(
                    good_data.split("\n \n")
                ))

        curr += 1
    raise PDFEOFException("There was no data found in PDF")
        


def find_covid_data(pdf_reader: PyPDF2.PdfFileReader) -> list:
    """Used to find the covid data in the PDF

    :param pdf_reader: reader that is pulling from pdf
    :param type: PyPDF2.PdfFileReader

    :returns: all the data parsed from pdf
    :return type: list
    """
    num_pages = pdf_reader.numPages
    for i in range(num_pages):
        page_data = pdf_reader.getPage(i).extractText()
        if "SURVEILLANCE" in page_data:
            try:
                return find_last_pages(i, num_pages, pdf_reader)
            except:
                break
    raise PDFNewFileFormatException("There is a new format used for the PDF by WHO.")
  

def scrape_pdf(who_pdf: str, who_csv:str) -> bool:
    """Used to scrape the PDF

    :param who_pdf: world heath orginzation pdf location relative
    :param type: str

    :param who_csv: world heath orginzation csv location relative
    :param type: str

    :returns: all the data parsed from pdf
    :return type: bool
    """
    data = None
    # read the pdf to prepare for scraping
    with open(f'{who_pdf}','rb') as f:
        try:
            data = find_covid_data(PyPDF2.PdfFileReader(f))
        except:
            return True

    # write the pdf to a csv formatted with headers
    with open(f'{who_csv}', 'w') as csv_file:
        file_writer = csv.writer(csv_file, delimiter=',',
            quotechar='|', quoting=csv.QUOTE_MINIMAL)
       
        file_writer.writerow(["country", "cases", "cases_new", 
            "deaths", "deaths_new", "transmisison_classification",
            "days_since_last_report"])

        for row in data:
            for item in row:
                # some checks to remove invalid data
                if item == [''] or item[0] == '': continue
                if item == [' \n'] or item == [' ']: continue
                
                # get rid of newlines in each row
                stripped = [x.replace('\n','') for x in item]
                if item == [' ']: continue

                file_writer.writerow(stripped)
    return False