def convert_string_to_float(value):
    """
    remove comma from string

    :param str value:
    :return: float value
    """
    return float(value.replace(',', ''))