import argparse
import random

def main(delimiter, num_columns, num_rows):
    vars = ["", ".", "foo", "bar", "trolls", "lol", "dayum", "5", "7"]
    with open("test_file.txt", "w+") as f:
        for j in range(num_rows):
            for i in range(num_columns):
                f.write(f"{vars[random.randint(0,len(vars)-1)] + delimiter}")
            f.write("\n")


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--delimiter", type=str, dest='delimiter', default=",")
    parser.add_argument("--num_columns", type=int, dest='nc', default=10)
    parser.add_argument("--num_rows", type=int, dest='nr', default=1000)
    args = vars(parser.parse_args())
    main(args['delimiter'], args['nc'], args['nr'])

