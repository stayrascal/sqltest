from sqltest.datasource import ExcelDatasetReader


def excel_reader(data_path: str):
    def wrapper(func):
        def decorate(*args):
            reader = ExcelDatasetReader(data_path)
            func(*args, reader)

        return decorate

    return wrapper
