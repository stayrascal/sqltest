from sqltest.datasource import ExcelDatasetReader


def excel_reader(data_path: str):
    def wrapper(func):
        def decorate(*args):
            print(f"Excel: {args} ")
            reader = ExcelDatasetReader(data_path)
            func(*args, reader)

        return decorate

    return wrapper
