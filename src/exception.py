import sys
from src.logger import logging


# method to extract error message details
def error_message_details(error,error_details_object:sys):
    # get the traceback information from the error details
    _,_,exc_tb = error_details_object.exc_info()

    # extract the filename ffrom the traceback
    filename = exc_tb.tb_frame.f_code.co_filename

    #create a formatted error message
    formatted_error_message = f'File : {{filename}} \nLine number: [{exc_tb.tb_lineno}] \nerror message: [{error}]'

    return formatted_error_message


#Define the custom exception
class FileOperationError(Exception):
    def __init__(self, formatted_error_message,error_details_object:sys):
        # calll a class constructor to set the errormessage
        super().__init__(formatted_error_message)
        self.formatted_error_message = error_message_details(formatted_error_message,error_details_object=error_details_object)

        # override the __str__ method to return the formatted error message

        def __str__(self):
            return self.formatted_error_message
        