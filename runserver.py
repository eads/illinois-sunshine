from sunshine import create_app
import logging

#logging.basicConfig(filename="error.log", level=logging.INFO)
app = create_app()

if __name__ == "__main__":
    import sys
    try:
        port = int(sys.argv[1])
    except (IndexError, ValueError):
        port = 5000
    app.run(debug=True, port=port)
