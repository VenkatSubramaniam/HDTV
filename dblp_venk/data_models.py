# for usage see parse_dblp.py

class Authorship:
    
    def __init__(self, writer, pubkey=None, author=None):
        self.pubkey = pubkey
        self.author = author
        self.writer = writer

    def save(self):
        self.writer.add_to_table("authorship", self.pubkey, self.author)


class Article:

    def __init__(self, writer, pubkey, title=None, journal=None, year=None):
        self.pubkey = pubkey
        self.title = title
        self.journal = journal
        self.year = year
        self.writer = writer

    def set_title(self, title):
        self.title = title

    def set_journal(self, journal):
        self.journal = journal
    
    def set_year(self, year):
        self.year = year

    def save(self):
        self.writer.add_to_table("article", self.pubkey, self.title, self.journal, self.year)


class InProceedings:

    def __init__(self, writer, pubkey, title=None, booktitle=None, year=None):
        self.writer = writer
        self.pubkey = pubkey
        self.title = title,
        self.booktitle = booktitle
        self.year = year

    def set_title(self, title):
        self.title = title

    def set_booktitle(self, title):
        self.booktitle = title
    
    def set_year(self, year):
        self.year = year

    def save(self):
        self.writer.add_to_table("inproceedings", self.pubkey, self.title, self.booktitle, self.year)
        
