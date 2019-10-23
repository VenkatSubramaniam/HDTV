#!/usr/bin/env python
# coding: utf-8
def write_stream():
    """Stream writer for a stream of XML inputs"""
    pass

def write_blocks(x, n, columns=True):
    """Block writer for an XML tree - no dynamic sizing"""
    # inevitable disaster prevention
    try:
        a1 = str(x.attrib['key'])
        a = a1.translate(str.maketrans({"'":"-"}))
    except:
        a = 'nulled'
    try:
        b1 = str(x.find('title').text)
        b = b1.translate(str.maketrans({"'":"-"}))
    except:
        b = 'nulled'
    try:
        c = int(x.find('year').text)
    except:
        c = 'null'   
    try:
        d1 = str(x.find('journal').text)
        d = d.translate(str.maketrans({"'":"-"}))
    except:
        d = 'nulled'  
    try:
        e1 = str(x.find('booktitle').text)
        e = e1.translate(str.maketrans({"'":"-"}))
    except:
        e = 'nulled'  

    if x.tag=='article':
        with open(f'Script Executables/articles_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
            f.write(f"INSERT INTO Articles (pubkey, title, journal, year) VALUES (\'{a}\', \'{b}\', \'{d}\', {c});\n")
            f.close()
    if x.tag=='inproceedings':
        with open(f'Script Executables/inproceedings_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
            f.write(f"INSERT INTO Inproceedings (pubkey, title, booktitle, year) VALUES (\'{a}\', \'{b}\', \'{e}\', {c});\n")
            f.close()
    with open(f'Script Executables/authorships_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
        for auth in x.findall('author')[:]:
            try:
                at1 = str(auth.text)
                at = at1.translate(str.maketrans({"'":"-"})).replace("'","")
            except:
                print('malformed author entry')
                print(f'could not parse at {auth}')
            f.write(f"INSERT INTO Authorships (pubkey, author) VALUES (\'{a}\', \'{at}\');\n")
        f.close()
    return



