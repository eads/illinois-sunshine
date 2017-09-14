import argparse, getpass, bcrypt
import sqlalchemy as sa
from sunshine.app_config import STOP_WORD_LIST
from sunshine.database import engine, Base

def CreateUser(args):
    connection = engine.connect()

    try:
        sql_params = {
            'username': args.username,
            'password': bcrypt.hashpw(args.password, bcrypt.gensalt()),
            'isadmin': args.is_admin
        }
        sql = """INSERT INTO users_table (username, password, is_admin, is_active, created_date, updated_date) VALUES (:username, :password, :isadmin, True, now(), now())"""
        connection.execute(sa.sql.text(sql), **sql_params)
    except sa.exc.ProgrammingError as e:
        print("Unexpected error encountered: ")
        print(e)

def main(args):
    if (args.command == 'CreateUser'):
        CreateUser(args)

class PasswordArgAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        mypass = getpass.getpass()
        setattr(namespace, self.dest, mypass)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Script for various admin commands.')
    subparsers = parser.add_subparsers(help='Admin Commands', dest='command')

    # CreateUser
    parser_createuser = subparsers.add_parser('CreateUser')
    parser_createuser.add_argument('-u', '--username', help='New user\'s username')
    parser_createuser.add_argument('-p', '--password', action=PasswordArgAction, nargs=0, help='New user\'s password')
    parser_createuser.add_argument('-a', '--is_admin', action='store_true', help='Flag indicating an admin')

    main(parser.parse_args())
