# for usage see parse_dblp.py

import xml.sax as sax
import re
from data_models import Authorship, Article, InProceedings

class DocHandler(sax.ContentHandler):
    def __init__(self, batch_size, writer):
        super().__init__()
        self.DBWriter = writer
        self.counter = 0
        self.batch_size = batch_size
        self.data_model = None
        self.curr_pubkey = None
        self.curr_author = None
        self.curr_year = None
        self.curr_title = None
        self.curr_booktitle = None
        self.curr_journal = None
        self.seen_authors = set()

    def startElement(self, name, attributes):
        if name == "article" or name == "inproceedings":
            self.curr_pubkey = attributes["key"]
            self.data_model = Article(self.DBWriter, pubkey=self.curr_pubkey) if name == "article" else InProceedings(self.DBWriter, pubkey=self.curr_pubkey)
        if self.curr_pubkey:
            if name == "author":
                self.curr_author = ""
            if name == "year":
                self.curr_year = ""
            if name == "title":
                self.curr_title = ""
            if name == "booktitle" and isinstance(self.data_model, InProceedings):
                self.curr_booktitle = ""
            if name == "journal" and isinstance(self.data_model, Article):
                self.curr_journal = ""

    def endElement(self, name):
        if name == "author" and self.curr_author and (self.curr_pubkey, self.curr_author) not in self.seen_authors:
            Authorship(self.DBWriter, self.curr_pubkey, self.curr_author).save()
            self.seen_authors.add((self.curr_pubkey, self.curr_author))
            self.curr_author = None
        if name == "year" and self.curr_year:
            self.data_model.set_year(self.curr_year)
            self.curr_year = None
        if name == "booktitle" and self.curr_booktitle:
            self.data_model.set_booktitle(self.curr_booktitle)
            self.curr_booktitle = None
        if name == "title" and self.curr_title:
            self.data_model.set_title(self.curr_title)
            self.curr_title = None
        if name == "journal" and self.curr_journal:
            self.data_model.set_journal(self.curr_journal)
            self.curr_journal = None
        if name == "article" or name == "inproceedings":
            self.data_model.save()
            self.data_model = None
            self.curr_pubkey = None
            self.counter +=1
            if self.counter == self.batch_size:
                self.DBWriter.flush()
                self.counter = 0

    def characters(self, chars):
        if self.curr_author is not None:
            self.curr_author += chars
        if self.curr_year is not None:
            self.curr_year += chars
        if self.curr_title is not None:
            self.curr_title += chars
        if self.curr_journal is not None:
            self.curr_journal += chars
        if self.curr_booktitle is not None:
            self.curr_booktitle += chars


class DBWriter():
    def __init__(self, cur, conn):
        self.cur = cur
        self.conn = conn
        self.tables = {}
        self.num_commits = 0 

    def add_to_table(self, table, *columns):
        if table not in self.tables:
            self.tables[table] = []
        self.tables[table].append(list(map(self.str_if_exists, columns)))

    @staticmethod
    def str_if_exists(s):
        return str(s) if s else None

    def flush(self):
        for table in self.tables:
            #self.cur.copy_from(TempFile(self.tables[table]), table)
            #self.tables[table] = []
            if table == "authorship":
                sql_command = "insert into authorship values (%s, %s);"
            elif table == "article":
                sql_command = "insert into article values (%s, %s, %s, %s);"
            else:
                sql_command = "insert into inproceedings values (%s, %s, %s, %s);"
            for row in self.tables[table]:
                self.cur.execute(sql_command, tuple(row))
            self.tables[table] = []
        self.conn.commit()
        self.num_commits += 1
        print(f"Finished {self.num_commits} commits!") 
