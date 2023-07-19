class DbtCloudException(Exception):
    pass

class BasicError(Exception):
    """ Base class for piperider errors. """

    def __init__(self, *args, **kwargs):
        self.message = args[0] if len(args) else ''
        self.hint = kwargs.get('hint') or ''

    def __str__(self):
        return self.message

    hint = ''
    message = ''

class DbtCloudConfigError(BasicError):
    def __init__(self, config_file):
        self.config_file = config_file
        pass

    message = "Configuration validation failed."
    hint = "Please execute command 'dbt-cloud --help' to move forward."
