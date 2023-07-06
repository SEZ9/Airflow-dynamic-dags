"""
提供s3相关的公共方法.
"""
import base64
import logging
import re

def convert_comma_separated_string_to_list(text: str) -> list:
    """将逗号分隔的字符串输入转换为python列表.
    """
    logging.info(f'Start to convert comma separate input string to list.')
    logging.info(f'Input text is: {text}.')

    try:
        output_list = [elem.strip() for elem in text.split(',') if elem.strip()!='']
    except Exception as err:
        raise Exception("Error while parsing the comma separated string input.")

    output_list_stringify = ",\n".join(output_list)
    logging.info(f'Output list is: [\n{output_list_stringify}\n]')
    return output_list

def convert_base64_to_string(text: str) -> str:
    return base64.b64decode(text).decode()

def convert_list_to_comma_separated_string(input_list: list) -> str:
    return ','.join(input_list)

def extract_filename_from_path(file_path: str) -> str:
    pattern = '^.*/([^/]*)$'
    return re.findall(pattern, file_path)[0]