import PyPDF2
import csv


def remove_bad_headers(data:str):
    headers = [
    'Territories\n**\n',
    'European Region',
    'South\n-\nEast Asia Region',
    'Eastern Mediterranean Region',
    'Region of the Americas',
    'African Region',
    'European Region ']

    for item in headers:
        try:
            fix = data.split(item)
            fix[1] = fix[1].lstrip()
            data = ''.join(fix)
        except IndexError:
            pass 

    return data

def chunker(data: list, chunksize=7):
    local = data
    chunks = []
    for i in range(0, len(local), chunksize):
        chunks.append(local[i:i+chunksize])

    return chunks

def find_last_pages(i: int, num_pages: int, pdf_reader):
    all_chunks = []
    curr = i
    while curr < num_pages:
        page_data = pdf_reader.getPage(curr).extractText()

        if "Grand total" in page_data:
            good_data = remove_bad_headers(page_data)
            last_chunk = chunker(good_data.split("\n \n"))

            for idx, item in enumerate(last_chunk):
                for sub_idx, sub_item in enumerate(item):
                    if sub_item == "Subtotal for all \nregions":
                        del last_chunk[idx:]

            all_chunks.append(last_chunk)
            return all_chunks
        else:
            if curr == i:
                good_data = remove_bad_headers(page_data)
                
                all_chunks.append(
                    chunker(good_data.split("Western Pacific Region")[1].split("\n \n")[1:]
                ))
                
            else:
                good_data = remove_bad_headers(page_data)
                all_chunks.append(chunker(
                    good_data.split("\n \n")
                ))
        curr += 1
    raise "NoEndOfDocument"
        

def find_covid_data(pdf_reader):
    num_pages = pdf_reader.numPages
    for i in range(num_pages):
        page_data = pdf_reader.getPage(i).extractText()
        if "SURVEILLANCE" in page_data:
            return find_last_pages(i, num_pages, pdf_reader)
    raise "ShitSamError"
  

def main():
    data = None
    with open('another3.pdf','rb') as f:
        data = find_covid_data(PyPDF2.PdfFileReader(f))

    with open('example.csv', 'w') as csv_file:
        file_writer = csv.writer(csv_file, delimiter=',',
            quotechar='|', quoting=csv.QUOTE_MINIMAL)
       
        file_writer.writerow(["country", "cases", "cases_new", 
            "deaths", "deaths_new", "transmisison_classification",
            "days_since_last_report"])
        for row in data:
            for item in row:

                # country = item[0]
                # total_confirmed = item[1]
                if item == [''] or item[0] == '': continue
                if item == [' \n'] or item == [' ']: continue
                # total_confirmed_new = item[2]
                # total_deaths = item[3]
                # total_new_deaths = item[4]
                # classification = item[5]
                # last_report = item[6]
                stripped = [x.replace('\n','') for x in item]
                if item == [' ']: continue

                print(stripped)
                file_writer.writerow(stripped)

if __name__ == "__main__":
    main()