import operator

class TxtParser:
    JSON_IDENTIFIER = "json"
    CSV_IDENTIFIER = "csv"
    TXT_IDENTIFIER = "txt"
    UNDEFINED = "undefined"
    DELIMITERS = [
                ",",
                "\t",
                "!",
                " ",
                ";",
                "|",
                "-",
                ]

    def __init__(self):
        self.distribution = {}

    @staticmethod
    def find_delimiter_distribution(filename):
        # Discrete counts as k,v pairs.
        for delim in delimiters:
            self.distribution[delim] = 0
        # Remove one at a time until only one delimiter left.
        with open(filename, "r") as f:
            while len(self.distribution) > 1:
                l = f.readline()
                for delim in self.distribution:
                    if not l.contains(delim):
                        del self.distribution[delim]
            

    def get_delimiter(filename):
        self.find_delimiter_distribution(filename)
        if not self.distribution:
            return TxtParser.JSON_IDENTIFIER
        return max(self.distribution.iteritems(), key=operator.itemgetter(1))[0]               

    def identify_filetype(filename: str) -> str:
        extension = filename.split(".")[1]
        if extension not in (TxtParser.JSON_IDENTIFIER, TxtParser.CSV_IDENTIFIER, TxtParser.TXT_IDENTIFIER):
            return TxtParser.UNDEFINED
        if extension == TxtParser.JSON_IDENTIFIER:
            return TxtParser.JSON_IDENTIFIER
        if extension == TxtParser.CSV_IDENTIFIER:
            return TxtParser.CSV_IDENTIFIER
        return TxtParser.CSV_IDENTIFIER if self.get_delimiter(filename) == "," else TxtParse.TXT_IDENTIFIER

