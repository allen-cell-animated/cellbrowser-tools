import argparse
import db_api
import db_ops
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='input name query')
    args = parser.parse_args()

    session_dict = {
        # 'root': 'http://test-aics-01',
        # 'root': 'http://bisque-00.corp.alleninstitute.org:8080',
        # 'root': 'http://10.128.62.104:8080',
        'root': 'http://10.128.62.104',
        'user': 'admin',
        'password': 'admin'
    }

    db_api.DbApi.setSessionInfo(session_dict)
    db_ops.deleteDuplicateImagesByName(args.input)

if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
