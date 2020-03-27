from application import application
import logging

if __name__ == "__main__":
    logging.basicConfig(filename='application.log',
                        filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    logging.info("cool")
    application.run(debug=True)
