import PyPDF2
import csv

def remove_bad_headers(data:str):
    '''Removes bad headers to ensure data can be parsed properly'''
    headers = [
    'Territories\n**\n',
    'European Region',
    'South\n-\nEast Asia Region',
    'Eastern Mediterranean Region',
    'Region of the Americas',
    'African Region',
    'European Region ']

    # used to check if the header is in the string
    for item in headers:
        try:
            fix = data.split(item)      # split the item because we don't need it
            fix[1] = fix[1].lstrip()    # strip the headers to fix the problem
            data = ''.join(fix)         # rejoin the string 
        except IndexError:
            pass 

    return data

def chunker(data: list, chunksize=7):
    '''This for breaking up the pdf into sections of data'''
    local = data
    chunks = []
    for i in range(0, len(local), chunksize):
        chunks.append(local[i:i+chunksize])

    return chunks

def find_last_pages(i: int, num_pages: int, pdf_reader):
    '''Grabs all the data from the pdf until it reaches last page'''
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
    raise "NoEndOfDocument"
        


def find_covid_data(pdf_reader):
    '''Used to grab pages from pdf to find table'''
    num_pages = pdf_reader.numPages
    for i in range(num_pages):
        page_data = pdf_reader.getPage(i).extractText()
        if "SURVEILLANCE" in page_data:
            return find_last_pages(i, num_pages, pdf_reader)
    raise "ShitSamError"
  

def scrape_pdf(NAME: str):

    data = None
    # read the pdf to prepare for scraping
    with open(f'{NAME}.pdf','rb') as f:
        data = find_covid_data(PyPDF2.PdfFileReader(f))

    # write the pdf to a csv formatted with headers
    with open(f'{NAME}.csv', 'w') as csv_file:
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