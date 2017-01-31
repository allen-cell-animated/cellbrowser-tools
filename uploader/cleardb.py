import argparse
import db_ops
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='input name query')
    args = parser.parse_args()
    db_ops.deleteImagesByName(args.input)

if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
