# code_verifier.py
import os
import pandas as pd 

OCCUPATION = "Occupation"
SOCP10 = "SOCP10"

class Checker(object):

    def __init__(self, sheetname=OCCUPATION):
        data_dir = os.path.dirname(__file__)
        filepath= "{}/data/valid_codes/CodeLists.xls".format(data_dir)

        df = pd.read_excel(filepath, sheetname=sheetname)
        df[SOCP10] = df[SOCP10].str.replace("-", "")
        self.df=df

    def lookup(self, code):
        return not self.df[self.df[SOCP10] == code].empty


if __name__ == '__main__':
    chk = Checker(OCCUPATION)
    print "Quick test..."
    assert not chk.lookup("Hello!")
    assert chk.lookup("111031")