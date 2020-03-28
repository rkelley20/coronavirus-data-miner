class PDFScrapeException(Exception):
    """Used for Scraping Errors"""
    def __init__(self, message):
        super().__init__(message)

class PDFEOFException(Exception):
    """Used EOF PDF Scraping Errors"""
    def __init__(self, message):
        super().__init__(message)

class PDFNewFileFormatException(Exception):
    """Used for new file structure PDF Scraping Errors"""
    def __init__(self, message):
        super().__init__(message)

class PDFNotFoundException(Exception):
"""Used when the PDF is not found scraping the data online"""
    def __init__(self, message):
        super().__init__(message)