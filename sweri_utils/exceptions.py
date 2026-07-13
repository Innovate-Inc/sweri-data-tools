class GdbNotFound(Exception):
    def __init__(self, fc_name, gdb_path, message="Geodatabase does not exist"):
        self.fc_name= fc_name
        self.gdb_path= gdb_path
        self.message = message

        # pass message to base Exception class
        super().__init__(self.message)

class GdbWontOpen(Exception):
    def __init__(self, gdb_path, message="Geodatabase could not be opened"):
        self.gdb_path = gdb_path
        self.message = message

        # Pass message to base Exception class
        super().__init__(self.message)


class FeatureClassNotFound(Exception):
    def __init__(self, fc_name, gdb_path, message="Feature class not found"):
        self.fc_name = fc_name
        self.gdb_path = gdb_path
        self.message = message

        # Pass message to base Exception class
        super().__init__(self.message)


class EmptyFeatureClass(Exception):
    def __init__(self, fc_name, gdb_path, message="Feature class has 0 features"):
        self.fc_name = fc_name
        self.gdb_path = gdb_path
        self.message = message

        # Pass message to base Exception class
        super().__init__(self.message)




