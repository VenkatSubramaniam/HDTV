import argparse
import random

def main(delimiter, num_columns, num_rows, malformed):
    vars = [".", "foo", "bar", "trolls", "lol", "dayum", "5", "7"]
    with open("test_file.txt", "w+") as f:
        for j in range(num_rows):
            for i in range(num_columns):
                if malformed and j%random.randint(1,40)==0 and i%(num_rows//3)==0:  # magic numbers I am using to malform the file in a seemingly random way
                    continue
                f.write(f"{vars[random.randint(0,len(vars)-1)]}")
                if i!=num_columns-1:
                    f.write(delimiter)
            f.write("\n")


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--delimiter", type=str, dest='delimiter', default=",")
    parser.add_argument("--num_columns", type=int, dest='nc', default=10)
    parser.add_argument("--num_rows", type=int, dest='nr', default=1000)
    parser.add_argument("--malformed",  action="store_true", default=False)
    args = vars(parser.parse_args())
    main(args['delimiter'], args['nc'], args['nr'], args['malformed'])

